package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.operator.OperatorStats;
import io.trino.spi.metrics.Metrics;
import io.trino.plugin.base.metrics.LongCount;

public class TestClickBenchFollowUps extends AbstractTestQueryFramework {
    private static final int ROWS = 10000;
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        QueryRunner runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.aggregation-group-limit", "100", "redisearch.narrow-scan-cursor-count", System.getProperty("followups.cursor-count", "10000")));
        StringBuilder schema = new StringBuilder("CREATE TABLE followups (userid bigint, url varchar, eventtime timestamp(3)");
        for (int i = 0; i < 102; i++) { schema.append(", c").append(i).append(" double"); }
        runner.execute(schema.append(")").toString());
        try (var writes = server.getClient().connect()) {
            writes.setAutoFlushCommands(false);
            for (int batch = 0; batch < 10; batch++) {
                var futures = new java.util.ArrayList<java.util.concurrent.CompletableFuture<?>>();
                for (int i = batch * 1000; i < (batch + 1) * 1000; i++) {
                    Map<String, String> fields = new HashMap<>();
                    fields.put("userid", Long.toString(9007199254740992L + i));
                    fields.put("url", i % 10 == 0 ? "https://google/" + i : "other/" + i);
                    fields.put("eventtime", Long.toString(1700000000000L + i));
                    for (int c = 0; c < 102; c++) { fields.put("c" + c, "0.1234567890123456"); }
                    futures.add(writes.async().hset("followups:" + i, fields).toCompletableFuture());
                }
                writes.flushCommands();
                java.util.concurrent.CompletableFuture.allOf(futures.toArray(java.util.concurrent.CompletableFuture[]::new)).join();
            }
        }
        server.awaitIndexed("followups");
        return runner;
    }

    private long metric(Metrics metrics, String name) { return ((LongCount) metrics.getMetrics().get(name)).getTotal(); }

    @Test
    public void testWideTimestampTopN() {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(),
                "SELECT * FROM followups WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10");
        assertThat(result.result().getRowCount()).isEqualTo(10);
        for (int i = 0; i < 10; i++) {
            var row = result.result().getMaterializedRows().get(i);
            assertThat(row.getField(0)).isEqualTo(9007199254740992L + i * 10);
            assertThat(row.getField(1)).isEqualTo("https://google/" + i * 10);
            assertThat(row.getField(2)).isEqualTo(java.time.LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(1700000000000L + i * 10), java.time.ZoneOffset.UTC));
            for (int c = 3; c < 105; c++) { assertThat(row.getField(c)).isEqualTo(0.1234567890123456); }
        }
        Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
        assertThat(metric(metrics, "redis.rows.received")).isEqualTo(1000);
        assertThat(metric(metrics, "redis.exact-hash-reads")).isEqualTo(1050);
        assertThat(metric(metrics, "redis.exact-hash-commands")).isEqualTo(10);
        System.out.println("FOLLOWUPS_TOPN bytes=" + getDistributedQueryRunner().getCoordinator().getQueryManager()
                .getFullQueryInfo(result.queryId()).getQueryStats().getPhysicalInputDataSize().toBytes()
                + " metrics=" + metrics.getMetrics());
        assertThat(query("SELECT count(*) FROM followups WHERE url LIKE '%google%' AND url LIKE '%/10%'")).matches("VALUES BIGINT '12'");
    }

    @Test
    public void testExactLookupAndDistinct() {
        assertThat(query("SELECT userid FROM followups WHERE userid = 9007199254740993")).matches("VALUES BIGINT '9007199254740993'");
        assertThat((String) computeActual("EXPLAIN " + "SELECT userid FROM followups WHERE userid = 9007199254740993").getOnlyValue()).doesNotContain("constraint=ALL");
        var selective = getDistributedQueryRunner().executeWithPlan(getSession(),
                "SELECT userid FROM followups WHERE userid = 9007199254740993");
        var queryStats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(selective.queryId()).getQueryStats();
        Metrics metrics = queryStats.getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
        assertThat(metric(metrics, "redis.rows.received")).isBetween(1L, 9L);
        assertThat(metric(metrics, "redis.exact-hash-reads")).isBetween(1L, 9L);
        System.out.println("FOLLOWUPS_LOOKUP rows=" + queryStats.getPhysicalInputPositions() + " metrics=" + metrics.getMetrics());
        assertThat(query("SELECT count(DISTINCT userid), count(DISTINCT url) FROM followups")).matches("VALUES (BIGINT '10000', BIGINT '10000')");
        assertThat(query("SELECT count(*) FROM (SELECT url, count(*) FROM followups GROUP BY url)")).matches("VALUES BIGINT '10000'");
        assertThat(query("SELECT count(*) FROM (SELECT 1, url, count(*) FROM followups GROUP BY 1, url)")).matches("VALUES BIGINT '10000'");
    }

    @Test
    public void testSignedExtremesDuplicatesAndNulls() {
        assertUpdate("CREATE TABLE signed_ids (id bigint, phrase varchar, marker varchar)");
        assertUpdate("INSERT INTO signed_ids VALUES (9007199254740992, 'a', 'row'), (9007199254740993, 'b', 'row'), "
                + "(9007199254740993, 'b', 'row'), (-9223372036854775808, '', 'row'), (9223372036854775807, 'A', 'row'), (NULL, NULL, 'row')", 6);
        assertThat(query("SELECT id FROM signed_ids WHERE id = 9007199254740993")).matches("VALUES BIGINT '9007199254740993', BIGINT '9007199254740993'");
        assertThat(query("SELECT count(*) FROM signed_ids WHERE id IN (9007199254740992, 9007199254740993)")).matches("VALUES BIGINT '3'");
        assertThat(query("SELECT id FROM signed_ids WHERE id IN (-9223372036854775808, 9223372036854775807)")).matches("VALUES BIGINT '-9223372036854775808', BIGINT '9223372036854775807'");
        assertThat(query("SELECT id FROM signed_ids WHERE id = 9007199254740994")).matches("SELECT CAST(NULL AS bigint) WHERE false");
        assertThat(query("SELECT count(DISTINCT id), count(DISTINCT phrase) FROM signed_ids")).matches("VALUES (BIGINT '4', BIGINT '4')");
        var redis = server.getConnection().sync();
        redis.ftCreate("json_signed", io.lettuce.core.search.arguments.CreateArgs.builder()
                .on(io.lettuce.core.search.arguments.CreateArgs.TargetType.JSON).withPrefix("json_signed:").build(),
                List.of(io.lettuce.core.search.arguments.NumericFieldArgs.builder().name("$.id").as("id").build()));
        RediSearchColumnTypes.write(redis, "json_signed", Map.of("id", io.trino.spi.type.BigintType.BIGINT));
        redis.jsonSet("json_signed:1", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":9007199254740992}");
        redis.jsonSet("json_signed:2", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":9007199254740993}");
        redis.jsonSet("json_signed:3", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":9223372036854775807}");
        server.awaitIndexed("json_signed");
        assertThat(query("SELECT id FROM json_signed WHERE id = 9007199254740993")).matches("VALUES BIGINT '9007199254740993'");
        assertThat(query("SELECT id FROM json_signed WHERE id IN (9007199254740992, 9223372036854775807)"))
                .matches("VALUES BIGINT '9007199254740992', BIGINT '9223372036854775807'");
    }

    @Test
    public void testTimestampTiesAndNulls() {
        assertUpdate("CREATE TABLE timestamp_ties (id bigint, t timestamp(3), url varchar)");
        assertUpdate("INSERT INTO timestamp_ties VALUES (1, TIMESTAMP '2020-01-01 00:00:00', 'google'), "
                + "(2, TIMESTAMP '2020-01-01 00:00:00', 'google'), (3, TIMESTAMP '2021-01-01 00:00:00', 'google'), "
                + "(4, NULL, 'google'), (5, TIMESTAMP '2010-01-01 00:00:00', 'Google'), (6, NULL, NULL)", 6);
        assertThat(query("SELECT id FROM timestamp_ties WHERE url LIKE '%google%' ORDER BY t LIMIT 2")).matches("VALUES BIGINT '1', BIGINT '2'");
        assertThat(computeActual("SELECT id FROM timestamp_ties WHERE url LIKE '%google%' ORDER BY t LIMIT 1").getOnlyValue())
                .isIn(1L, 2L);
        assertThat(query("SELECT id FROM timestamp_ties WHERE url LIKE '%google%' ORDER BY t DESC LIMIT 1")).matches("VALUES BIGINT '3'");
        assertThat(query("SELECT id FROM timestamp_ties WHERE url LIKE '%google%' ORDER BY t NULLS FIRST LIMIT 1")).matches("VALUES BIGINT '4'");
        server.getConnection().sync().hset("timestamp_ties:large1", Map.of("id", "7", "t", "9007199254740992", "url", "google"));
        server.getConnection().sync().hset("timestamp_ties:large2", Map.of("id", "8", "t", "9007199254740993", "url", "google"));
        assertThat(query("SELECT id FROM timestamp_ties WHERE url LIKE '%google%' ORDER BY t DESC LIMIT 1"))
                .matches("VALUES BIGINT '8'");
        assertUpdate("CREATE TABLE timestamp_spellings (id bigint, t timestamp(3))");
        server.getConnection().sync().hset("timestamp_spellings:1", Map.of("id", "1", "t", "1.0"));
        server.getConnection().sync().hset("timestamp_spellings:2", Map.of("id", "2", "t", "2e0"));
        assertThat(query("SELECT id FROM timestamp_spellings ORDER BY t LIMIT 1")).matches("VALUES BIGINT '1'");
        server.getConnection().sync().hset("timestamp_spellings:3", Map.of("id", "3", "t", Long.toString(Long.MAX_VALUE)));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> computeActual("SELECT id FROM timestamp_spellings ORDER BY t LIMIT 1"))
                .hasMessageContaining("long overflow");
    }

    @Test
    public void benchmarkNarrowScans() {
        org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.getBoolean("followups.benchmark"));
        for (String column : List.of("userid", "url")) {
            for (int attempt = 1; attempt <= 3; attempt++) {
                long start = System.nanoTime();
                var result = getDistributedQueryRunner().executeWithPlan(getSession(),
                        "SELECT count(DISTINCT " + column + ") FROM followups");
                double wallMillis = (System.nanoTime() - start) / 1e6;
                assertThat(result.result().getOnlyValue()).isEqualTo((long) ROWS);
                var stats = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getQueryStats();
                Metrics metrics = stats.getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                        .reduce(Metrics.EMPTY, Metrics::mergeWith);
                System.out.println("FOLLOWUPS_BENCH count=" + System.getProperty("followups.cursor-count", "10000")
                        + " column=" + column + " attempt=" + attempt + " wall_ms=" + wallMillis
                        + " cpu_ms=" + stats.getTotalCpuTime().toMillis() + " bytes=" + stats.getPhysicalInputDataSize().toBytes()
                        + " rows=" + stats.getPhysicalInputPositions() + " peak_trino_bytes=" + stats.getPeakUserMemoryReservation().toBytes()
                        + " metrics=" + metrics.getMetrics());
            }
        }
    }

    @Test
    public void testGroupBudgetBoundary() {
        assertThat(RediSearchMetadata.isGroupPushdownSafe(OptionalLong.empty(), 100)).isFalse();
        assertThat(RediSearchMetadata.isGroupPushdownSafe(OptionalLong.of(100), 100)).isTrue();
        assertThat(RediSearchMetadata.isGroupPushdownSafe(OptionalLong.of(101), 100)).isFalse();
    }
}
