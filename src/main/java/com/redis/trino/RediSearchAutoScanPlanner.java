package com.redis.trino;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.airlift.log.Logger;
import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.QueryDialects;
import io.lettuce.core.search.arguments.SearchArgs;

/** Bounded, cached cost estimates for automatic scan parallelism. Estimates never define row coverage. */
final class RediSearchAutoScanPlanner {
    private static final Logger log = Logger.get(RediSearchAutoScanPlanner.class);
    static final long ROWS_PER_SPLIT = 250_000;
    private final RediSearchSession session;
    private final IntSupplier capacity;
    private final Cache<String, List<RediSearchScanPartition>> cache = CacheBuilder.newBuilder()
            .maximumSize(256).expireAfterWrite(Duration.ofMinutes(5)).build();

    RediSearchAutoScanPlanner(RediSearchSession session, IntSupplier capacity) {
        this.session = session;
        this.capacity = capacity;
    }

    static int splitCount(long rows, long connections, int cores) {
        return (int) Math.max(1, Math.min(Math.min(64, connections),
                Math.min(Math.max(1, cores), rows / ROWS_PER_SPLIT)));
    }

    List<RediSearchScanPartition> plan(RediSearchTableHandle table, RediSearchIndexInfo info) {
        var config = session.getConfig();
        long rows = info.getNumDocs().orElse(0);
        int count = splitCount(rows, config.getScanConnections(), capacity.getAsInt());
        log.debug("Automatic scan sizing for %s: %s rows, %s splits", table.getIndex(), rows, count);
        if (count < 2 || !RediSearchScanPartition.isDocumentScan(table)) {
            return List.of();
        }
        try {
            // A point lookup or selective scan should not fan out across the whole index.
            String query = new RediSearchQueryBuilder().buildQuery(table.getConstraint());
            if (!query.equals("*")) {
                rows = count(table.getIndex(), query);
                count = splitCount(rows, config.getScanConnections(), capacity.getAsInt());
                if (count < 2) { return List.of(); }
            }
            int selected = count;
            long estimatedRows = rows;
            String key = table.getIndex() + ":" + count + ":" + query + ":" + info.getFields().stream()
                    .map(field -> field.getAttribute() + "/" + field.getType() + "/" + field.isIndexed()).toList();
            return cache.get(key, () -> {
                try { return discover(table, info, selected, estimatedRows, query); }
                catch (RuntimeException failure) {
                    log.info("Partition statistics unavailable for %s: %s", table.getIndex(), failure.toString());
                    return List.of();
                }
            });
        } catch (Exception failure) {
            // Unsupported commands, timeouts, partial statistics, or unusual numeric values affect only the estimate.
            log.debug(failure, "Automatic split discovery unavailable for %s; using one split", table.getIndex());
            return List.of();
        }
    }

    private List<RediSearchScanPartition> discover(RediSearchTableHandle table, RediSearchIndexInfo info, int count, long rows, String query) {
        var config = session.getConfig();
        if (config.getScanPartitionField() != null && !config.getScanPartitionBoundaries().isEmpty()) {
            return RediSearchScanPartition.plan(new RediSearchConfig().setScanSplits(count)
                    .setScanConnections(config.getScanConnections()).setScanPartitionField(config.getScanPartitionField())
                    .setScanPartitionBoundaries(config.getScanPartitionBoundaries()), table, info);
        }
        // Scalar HASH fields have well-defined MIN/MAX statistics. JSON arrays still support explicit cut points.
        if (info.getKeyType().orElse(null) != RediSearchIndexInfo.KeyType.HASH) { return List.of(); }
        var fields = info.getFields().stream().filter(field -> field.isIndexed()
                && field.getType() == RediSearchFieldType.NUMERIC && RediSearchQueryBuilder.isProperty(field.getAttribute())
                && (config.getScanPartitionField() == null || config.getScanPartitionField().equals(field.getAttribute())))
                .limit(3).toList();
        if (fields.isEmpty()) { return List.of(); }
        var group = new AggregateArgs.GroupBy(List.of());
        var args = AggregateArgs.builder().timeout(Duration.ofSeconds(10)).dialect(QueryDialects.DIALECT2);
        for (int i = 0; i < fields.size(); i++) {
            String field = "@" + fields.get(i).getAttribute();
            args.load(field);
            group.reduce(AggregateArgs.Reducer.min(field).as("__min_" + i));
            group.reduce(AggregateArgs.Reducer.max(field).as("__max_" + i));
        }
        var reply = session.sync().ftAggregate(table.getIndex(), query, args.groupBy(group).build());
        if (reply.getReplies().stream().anyMatch(part -> !part.getWarnings().isEmpty())) {
            throw new IllegalStateException("Incomplete partition statistics");
        }
        var statistics = reply.getReplies().stream().flatMap(part -> part.getResults().stream()).toList();
        if (statistics.size() != 1) { return List.of(); }
        var values = statistics.getFirst().getFields();
        for (int i = 0; i < fields.size(); i++) {
            double min;
            double max;
            try {
                min = Double.parseDouble(values.get("__min_" + i).asString());
                max = Double.parseDouble(values.get("__max_" + i).asString());
            } catch (NumberFormatException | NullPointerException missing) { continue; }
            if (!Double.isFinite(min) || !Double.isFinite(max) || min >= max) { continue; }
            List<Double> cuts = new ArrayList<>();
            for (int n = 1; n < count; n++) {
                // Weighted endpoints avoid overflowing max-min for very large finite doubles.
                double cut = min * (1.0 - (double) n / count) + max * ((double) n / count);
                if (Double.isFinite(cut) && (cuts.isEmpty() || cut > cuts.getLast())) { cuts.add(cut); }
            }
            String field = fields.get(i).getAttribute();
            var candidate = RediSearchScanPartition.plan(new RediSearchConfig().setScanSplits(count)
                    .setScanConnections(config.getScanConnections()).setScanPartitionField(field)
                    .setScanPartitionBoundaries(cuts), table, info);
            long largest = 0;
            for (var partition : candidate) { largest = Math.max(largest, count(table.getIndex(), query.equals("*") ? partition.query() : "(" + query + ") (" + partition.query() + ")")); }
            // Reject skew that leaves at least 80% of work on one source; try another indexed field.
            if (candidate.size() > 1 && largest < rows * 0.8) { return candidate; }
        }
        return List.of();
    }

    private long count(String index, String query) {
        var reply = session.sync().ftSearch(index, query, SearchArgs.<String>builder().limit(0, 0)
                .timeout(Duration.ofSeconds(1)).dialect(QueryDialects.DIALECT2).build());
        if (!reply.getWarnings().isEmpty()) { throw new IllegalStateException("Incomplete partition count"); }
        return reply.getCount();
    }
}
