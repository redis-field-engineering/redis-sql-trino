package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.DurationTiming;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.DistributedQueryRunner;

/** Manual, read-only sweep over an isolated preloaded ClickBench sample. */
public class RediSearchParallelScanBenchmark {
    @Test
    public void benchmark() throws Exception {
        String uri = System.getProperty("benchmark.redis-uri");
        assumeTrue(uri != null, "Manual benchmark requires benchmark.redis-uri");
        var codec = JsonCodec.mapJsonCodec(String.class, Object.class);
        Map<String, Object> reference = codec.fromJson(Files.readString(Path.of("benchmarks/issue-142/sample.json")));
        List<Map<String, Object>> samples = new ArrayList<>();
        for (int splits : java.util.Arrays.stream(System.getProperty("benchmark.splits", "1,2,4,8").split(","))
                .map(Integer::parseInt).toList()) {
            try (var runner = DistributedQueryRunner.builder(RediSearchQueryRunner.createSession()).setWorkerCount(2)
                    .build()) {
                runner.installPlugin(new RediSearchPlugin());
                var properties = new java.util.HashMap<>(Map.of(
                        "redisearch.uri", uri, "redisearch.default-schema-name", "tpch",
                        "redisearch.scan-connections", "8", "redisearch.scan-splits", String.valueOf(splits),
                        "redisearch.scan-partition-field", "ordinal",
                        "redisearch.scan-partition-boundaries", "625000,1250000,1875000,2500000,3125000,3750000,4375000",
                        "redisearch.query-timeout-ms", "1200000", "redisearch.cursor-count", "1000"));
                if (splits == 0) {
                    properties.remove("redisearch.scan-splits");
                    properties.remove("redisearch.scan-connections");
                    properties.remove("redisearch.scan-partition-field");
                    properties.remove("redisearch.scan-partition-boundaries");
                }
                runner.createCatalog("redisearch", "redisearch", properties);
                Files.writeString(Path.of("benchmarks/issue-142/plan-" + splits + ".txt"),
                        runner.execute("EXPLAIN SELECT COUNT(DISTINCT UserID) FROM hits").getOnlyValue().toString());
                var client = io.lettuce.core.RedisClient.create(uri);
                try (var point = client.connect()) {
                List<Double> idlePointMillis = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    long start = System.nanoTime();
                    assertThat(point.sync().ftSearch("hits", "@ordinal:[2500000 2500000]").getCount()).isEqualTo(1);
                    idlePointMillis.add((System.nanoTime() - start) / 1e6);
                }
                for (int attempt = 0; attempt < Integer.getInteger("benchmark.attempts", 4); attempt++) {
                    for (String name : List.of("scan", "q5", "q6", "q3")) {
                        String sql = switch (name) {
                            case "scan" -> "SELECT sum(length(SearchPhrase)) FROM hits";
                            case "q5" -> "SELECT COUNT(DISTINCT UserID) FROM hits";
                            case "q6" -> "SELECT COUNT(DISTINCT SearchPhrase) FROM hits";
                            default -> "SELECT SUM(AdvEngineID) FROM hits";
                        };
                        List<Double> pointMillis = java.util.Collections.synchronizedList(new ArrayList<>());
                        var done = new java.util.concurrent.atomic.AtomicBoolean();
                        var probe = Thread.ofPlatform().unstarted(() -> {
                            while (!done.get()) {
                                long pointStart = System.nanoTime();
                                assertThat(point.sync().ftSearch("hits", "@ordinal:[2500000 2500000]").getCount()).isEqualTo(1);
                                pointMillis.add((System.nanoTime() - pointStart) / 1e6);
                                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                            }
                        });
                        double redisCpuStart = cpuSeconds(point.sync().info("cpu"));
                        probe.start();
                        long cpuStart = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
                        long start = System.nanoTime();
                        io.trino.testing.QueryRunner.MaterializedResultWithPlan result;
                        long completedNanos;
                        try {
                            result = runner.executeWithPlan(runner.getDefaultSession(), sql);
                            completedNanos = System.nanoTime();
                        } finally { done.set(true); probe.join(); }
                        double elapsedMillis = (completedNanos - start) / 1e6;
                        long cpuNanos = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() - cpuStart;
                        assertThat(((Number) result.result().getOnlyValue()).longValue()).isEqualTo(((Number) reference.get(name)).longValue());
                        var info = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());
                        var stats = info.getQueryStats();
                        Metrics metrics = stats.getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                                .reduce(Metrics.EMPTY, Metrics::mergeWith);
                        Map<String, Object> sample = new LinkedHashMap<>();
                        sample.put("splits", splits); sample.put("attempt", attempt); sample.put("query", name);
                        sample.put("result", result.result().getOnlyValue()); sample.put("elapsed_ms", elapsedMillis);
                        sample.put("redis_cpu_cores", (cpuSeconds(point.sync().info("cpu")) - redisCpuStart) / (elapsedMillis / 1000));
                        sample.put("point_idle_median_ms", percentile(idlePointMillis, 0.5));
                        sample.put("point_scan_median_ms", percentile(pointMillis, 0.5));
                        sample.put("point_scan_p95_ms", percentile(pointMillis, 0.95));
                        sample.put("point_samples", pointMillis.size());
                        sample.put("trino_cpu_ms", stats.getTotalCpuTime().getValue(TimeUnit.MILLISECONDS));
                        sample.put("jvm_cpu_cores", cpuNanos / (elapsedMillis * 1e6));
                        sample.put("input_bytes", stats.getProcessedInputDataSize().toBytes());
                        sample.put("source_worker_count", io.trino.execution.StagesInfo.getAllStages(info.getStages()).stream()
                                .flatMap(stage -> stage.tasks().stream()).filter(task -> task.stats().pipelines().stream()
                                        .flatMap(pipeline -> pipeline.getOperatorSummaries().stream())
                                        .anyMatch(operator -> !operator.getConnectorMetrics().getMetrics().isEmpty()))
                                .map(task -> task.taskStatus().nodeId()).distinct().count());
                        sample.put("source_drivers", stats.getOperatorSummaries().stream()
                                .filter(op -> !op.getConnectorMetrics().getMetrics().isEmpty()).mapToLong(OperatorStats::getTotalDrivers).sum());
                        metrics.getMetrics().forEach((key, value) -> sample.put(key,
                                value instanceof LongCount counter ? counter.getTotal()
                                        : ((DurationTiming) value).getDuration().toNanos() / 1e6));
                        samples.add(sample);
                        Files.writeString(Path.of(System.getProperty("benchmark.output", "benchmarks/issue-142/results.json")), codec.toJson(Map.of("samples", samples)));
                        System.out.println("PARALLEL_SCAN " + codec.toJson(sample));
                    }
                }
                } finally { client.shutdown(); }
            }
        }
    }

    @Test
    public void automaticPlan() {
        String uri = System.getProperty("benchmark.plan-uri");
        assumeTrue(uri != null, "Manual automatic-planner probe");
        var unusedTypes = (io.trino.spi.type.TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                io.trino.spi.type.TypeManager.class.getClassLoader(), new Class<?>[] {io.trino.spi.type.TypeManager.class},
                (proxy, method, args) -> { throw new UnsupportedOperationException(); });
        var session = new RediSearchSession(unusedTypes, new RediSearchConfig().setUri(uri));
        try {
            var keyword = new io.lettuce.core.protocol.ProtocolKeyword() {
                public byte[] getBytes() { return "FT.INFO".getBytes(java.nio.charset.StandardCharsets.US_ASCII); }
            };
            List<Object> raw = session.sync().dispatch(keyword,
                    new io.lettuce.core.output.NestedMultiOutput<>(io.lettuce.core.codec.StringCodec.UTF8),
                    new io.lettuce.core.protocol.CommandArgs<>(io.lettuce.core.codec.StringCodec.UTF8).add("hits"));
            var table = new RediSearchTableHandle(new io.trino.spi.connector.SchemaTableName("tpch", "hits"), "hits");
            var partitions = new RediSearchAutoScanPlanner(session, () -> 10).plan(table, RediSearchIndexInfo.parse(raw));
            assertThat(partitions).hasSize(8);
        } finally { session.shutdown(); }
    }

    private static double percentile(List<Double> values, double percentile) {
        var sorted = values.stream().sorted().toList();
        return sorted.get(Math.min(sorted.size() - 1, (int) (percentile * sorted.size())));
    }

    private static double cpuSeconds(String info) {
        return info.lines().filter(line -> line.startsWith("used_cpu_sys:") || line.startsWith("used_cpu_user:"))
                .mapToDouble(line -> Double.parseDouble(line.substring(line.indexOf(':') + 1))).sum();
    }
}
