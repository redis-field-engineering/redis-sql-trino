package com.redis.trino;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

import io.lettuce.core.search.AggregationReply.Cursor;

/** Bounded timestamp TopN: scan exact keys/order values, then hydrate only the winners. */
final class RediSearchLocalTopN {
    private RediSearchLocalTopN() {}

    static Comparator<String[]> comparator(boolean ascending) {
        return (left, right) -> {
            String a = left[1];
            String b = right[1];
            if (a == null || b == null) { return a == b ? 0 : a == null ? 1 : -1; }
            int comparison = Long.compare(Long.parseLong(a), Long.parseLong(b));
            return ascending ? comparison : -comparison;
        };
    }

    static RediSearchSession.AggregateResult read(RediSearchSession session, RediSearchSession.Connection scan,
            RediSearchTableHandle table, List<RediSearchColumnHandle> outputs,
            RediSearchSession.ExactHashReader exactReader, RediSearchReadStats stats) {
        RediSearchSortItem sort = table.getSort().getFirst();
        RediSearchTable schema = session.getTable(table.getSchemaTableName());
        RediSearchColumnHandle order = schema.getColumns().stream().filter(column -> column.getName().equals(sort.getColumn()))
                .findFirst().orElseThrow();
        List<RediSearchColumnHandle> selection = List.of(RediSearchBuiltinField.KEY.getColumnHandle(), order);
        RediSearchTableHandle narrow = table.withoutTopN();
        Comparator<String[]> comparator = comparator(sort.isAscending());
        int limit = Math.toIntExact(table.getLimit().orElseThrow());
        PriorityQueue<String[]> winners = new PriorityQueue<>(limit, comparator.reversed());
        long started = System.nanoTime();
        Optional<Cursor> cursor = Optional.empty();
        try {
            RediSearchSession.AggregateResult batch = session.aggregate(scan, narrow, selection, exactReader, stats);
            cursor = batch.getCursor();
            while (true) {
                long timeoutMillis = session.getConfig().getQueryTimeoutMillis();
                if (timeoutMillis > 0 && java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= timeoutMillis) {
                    throw new io.trino.spi.TrinoException(RediSearchErrorCode.REDISEARCH_INCOMPLETE_RESULT,
                            "Timestamp TopN selection exceeded query-timeout-ms; no candidate rows were returned");
                }
                if (Thread.currentThread().isInterrupted()) { throw new java.util.concurrent.CancellationException(); }
                for (String[] row : batch.getRows()) {
                    // Use the scan writer's exact parser and range checks, including integral decimal/scientific
                    // spellings written by other clients. Validate even a row that won't enter the heap.
                    if (row[1] != null) { row[1] = Long.toString(RediSearchPageSourceResultWriter.timestampMicros(row[1])); }
                    if (winners.size() < limit) { winners.add(row); }
                    else if (comparator.compare(row, winners.peek()) < 0) { winners.remove(); winners.add(row); }
                }
                if (cursor.isEmpty()) { break; }
                var reply = session.cursorReadAsync(scan, narrow, cursor.orElseThrow(), stats, batch.getReader().getCursorCount()).join();
                Optional<Cursor> next = RediSearchSession.nextCursor(reply, cursor);
                try { batch = session.result(exactReader, batch.getReader(), cursor, reply, stats); }
                finally { cursor = next; }
            }
        } finally {
            cursor.ifPresent(current -> session.cursorDeleteAsync(scan, narrow, current));
        }
        List<String[]> candidates = new ArrayList<>(winners);
        candidates.sort(comparator);
        List<RediSearchColumnHandle> columns = RediSearchTranslator.readColumns(outputs);
        Map<String, String> identifiers = new HashMap<>();
        schema.getIndexInfo().getFields().forEach(field -> identifiers.put(field.getAttribute(), field.getIdentifier()));
        List<String[]> rows = new ArrayList<>();
        List<CompletableFuture<?>> reads = new ArrayList<>();
        for (String[] candidate : candidates) {
            String[] row = new String[columns.size()];
            List<String> fields = new ArrayList<>();
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i).getName();
                if (RediSearchBuiltinField.isKeyColumn(name)) { row[i] = candidate[0]; continue; }
                fields.add(identifiers.getOrDefault(name, name));
                positions.add(i);
            }
            if (!fields.isEmpty()) {
                stats.exactHashReads.add(fields.size());
                stats.exactHashCommands.increment();
                reads.add(exactReader.readFields(candidate[0], fields).thenAccept(values -> {
                    for (int i = 0; i < values.size(); i++) {
                        row[positions.get(i)] = values.get(i).hasValue() ? values.get(i).getValue() : null;
                    }
                }));
            }
            rows.add(row);
        }
        if (!reads.isEmpty()) {
            stats.exactHashReadBatches.increment();
            long start = System.nanoTime();
            try { exactReader.flush(); CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).join(); }
            finally { stats.exactWaitNanos.add(System.nanoTime() - start); }
        }
        RediSearchRowReader reader = new RediSearchRowReader(columns.stream().map(RediSearchColumnHandle::getName).toList(),
                Map.of(), Map.of(), java.util.Set.of(), List.of(), Map.of(), Map.of(), Optional.of(outputs));
        rows.replaceAll(reader::project);
        return new RediSearchSession.AggregateResult(rows, Optional.empty(), reader);
    }
}
