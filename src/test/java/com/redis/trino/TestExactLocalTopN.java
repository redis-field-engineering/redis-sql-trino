package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/** Exact ordering and evidence that wide fields are fetched only for retained candidates. */
public class TestExactLocalTopN extends AbstractTestQueryFramework {
    private static final int ROWS = 1107;
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        Map<String, String> properties = new HashMap<>(server.getConnectorProperties());
        properties.put("redisearch.default-schema-name", "tpch");
        properties.put("redisearch.cursor-count", "37");
        properties.put("redisearch.query-timeout-ms", "30000");
        QueryRunner runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.cursor-count", "37", "redisearch.query-timeout-ms", "30000"));
        properties.put("redisearch.local-topn.max-candidates", "0");
        runner.createCatalog("no_local_topn", "redisearch", properties);
        properties.put("redisearch.local-topn.max-candidates", "2000");
        runner.createCatalog("larger_topn", "redisearch", properties);
        runner.execute("CREATE TABLE topn_wide (id bigint, amount decimal(30,6), speed integer, v varchar, d date, "
                + "ts timestamp(12), tz timestamp(12) with time zone, ch char(4), a array(bigint), payload varchar)");
        runner.execute("INSERT INTO topn_wide SELECT BIGINT '9007199254740992' + n, "
                + "CASE WHEN n % 13 = 0 THEN NULL ELSE CAST(DECIMAL '9007199254740993.000000' + n * DECIMAL '0.000001' AS decimal(30,6)) END, "
                + "CAST(n % 23 AS integer), CASE WHEN n % 11 = 0 THEN NULL WHEN n % 3 = 0 THEN '\uE000' WHEN n % 3 = 1 THEN '\uD800\uDC00' ELSE 'same' END, "
                + "date_add('day', n % 31, DATE '1969-12-20'), "
                + "CASE WHEN n % 17 = 0 THEN NULL ELSE TIMESTAMP '1969-12-31 23:59:59.000000000001' + n * INTERVAL '1' SECOND END, "
                + "TIMESTAMP '2024-01-01 00:00:00.000000000001 UTC' + n * INTERVAL '1' SECOND, "
                + "CAST(CASE WHEN n % 2 = 0 THEN 'a' ELSE 'b ' END AS char(4)), ARRAY[n % 7, NULL], array_join(repeat('p', 2048), '') "
                + "FROM UNNEST(sequence(BIGINT '0', BIGINT '" + (ROWS - 1) + "')) t(n)");
        return runner;
    }

    @Test
    public void testMultiKeyNullOrderingUnicodeAndExactDecimals() {
        for (String order : List.of("amount DESC NULLS FIRST, id ASC", "v ASC NULLS FIRST, amount DESC NULLS LAST, id ASC",
                "v DESC NULLS LAST, amount ASC NULLS FIRST, id DESC", "d DESC, ts ASC NULLS FIRST, id ASC",
                "ts DESC NULLS FIRST, tz ASC, id ASC", "a ASC NULLS LAST, ch DESC, id ASC")) {
            String sql = "SELECT id, amount, v, d, ts, tz, ch, a FROM topn_wide ORDER BY " + order + " LIMIT 7";
            assertExactOrder(sql, baseline(sql));
            assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isEqualTo(7);
        }
    }

    @Test
    public void testWideDecimalTopNMaterializesOnlyWinnerProjections() {
        String sql = "SELECT id, payload FROM topn_wide ORDER BY amount ASC NULLS FIRST, id DESC LIMIT 5";
        assertExactOrder(sql, baseline(sql));
        Metrics metrics = metrics(sql);
        assertThat(metric(metrics, "redis.rows.received")).isEqualTo(ROWS);
        assertThat(metric(metrics, "redis.local-topn.candidates")).isEqualTo(5);
        // BIGINT and DECIMAL keys share one HMGET per source row, without fetching the wide payload.
        assertThat(metric(metrics, "redis.local-topn.sort-hash-reads")).isEqualTo(2L * ROWS);
        // The final Trino sort still needs amount + id, and the client also requests payload: 3 fields per winner.
        assertThat(metric(metrics, "redis.local-topn.projected-hash-reads")).isEqualTo(15);
        assertThat(metric(metrics, "redis.exact-hash-reads")).isEqualTo(2L * ROWS + 15);
        assertThat(metric(metrics, "redis.exact-hash-commands")).isEqualTo(ROWS + 5);
        assertThat(metric(metrics, "redis.exact-hash-read-batches")).isEqualTo(31);
        System.out.println("EXACT_TOPN_LATE_MATERIALIZATION deployment=" + deployment() + " metrics=" + metrics.getMetrics());
    }

    @Test
    public void testCoalescedNumericReadsResolveAliasesNullsAndInvalidLosers() {
        var redis = server.getConnection().sync();
        redis.ftCreate("topn_mixed_alias", io.lettuce.core.search.arguments.CreateArgs.builder()
                .withPrefix("topn_mixed_alias:").skipInitialScan().build(), List.of(
                io.lettuce.core.search.arguments.NumericFieldArgs.builder().name("raw_id").as("id").build(),
                io.lettuce.core.search.arguments.NumericFieldArgs.builder().name("raw_amount").as("amount").build(),
                io.lettuce.core.search.arguments.TagFieldArgs.builder().name("raw_payload").as("payload").build()));
        RediSearchColumnTypes.write(redis, "topn_mixed_alias", Map.of("id", io.trino.spi.type.BigintType.BIGINT,
                "amount", io.trino.spi.type.DecimalType.createDecimalType(30, 6),
                "payload", io.trino.spi.type.VarcharType.VARCHAR));
        for (int i = 0; i < 7; i++) {
            Map<String, String> fields = new HashMap<>();
            if (i != 0) { fields.put("raw_id", Long.toString(9007199254740992L + i)); }
            if (i > 1) { fields.put("raw_amount", "9007199254740993.00000" + i); }
            fields.put("raw_payload", "row-" + i);
            redis.hset("topn_mixed_alias:" + i, fields);
        }
        try {
            String sql = "SELECT id, amount, payload FROM topn_mixed_alias ORDER BY amount ASC NULLS FIRST, id DESC NULLS LAST LIMIT 3";
            assertExactOrder(sql, sql.replace("FROM topn_mixed_alias", "FROM no_local_topn.tpch.topn_mixed_alias"));
            Metrics metrics = metrics(sql);
            assertThat(metric(metrics, "redis.rows.received")).isEqualTo(7);
            assertThat(metric(metrics, "redis.local-topn.sort-hash-reads")).isEqualTo(14);
            assertThat(metric(metrics, "redis.local-topn.projected-hash-reads")).isEqualTo(9);
            assertThat(metric(metrics, "redis.exact-hash-commands")).isEqualTo(10);
            // This numeric index value is valid to Redis but not to SQL BIGINT. Even an obvious loser must decode.
            redis.hset("topn_mixed_alias:invalid", Map.of("raw_id", "1.5", "raw_amount", "9007199254740994", "raw_payload", "loser"));
            assertQueryFails(sql, ".*not a valid bigint.*");
        } finally {
            assertUpdate("DROP TABLE topn_mixed_alias");
        }
    }

    @Test
    public void testResidualArrayPredicateRunsBeforeTopNCandidatePruning() {
        String sql = "SELECT id, payload FROM topn_wide WHERE a[1] = BIGINT '0' "
                + "ORDER BY amount ASC NULLS FIRST, id DESC LIMIT 7";
        assertExactOrder(sql, baseline(sql));
        // NULL amounts come first. Among those, matching n values are multiples of both 13 and 7.
        assertThat(computeActual(sql).getMaterializedRows().stream().map(row -> row.getField(0)).toList())
                .containsExactly(9007199254742084L, 9007199254741993L, 9007199254741902L,
                        9007199254741811L, 9007199254741720L, 9007199254741629L, 9007199254741538L);
        Metrics selected = metrics(sql);
        assertThat(metric(selected, "redis.rows.received")).isEqualTo(ROWS);
        assertThat(metric(selected, "redis.local-topn.candidates")).isZero();
        assertThat(metric(selected, "redis.local-topn.projected-hash-reads")).isZero();
    }

    @Test
    public void testExactStringPredicateRunsBeforeLocalTopNCandidateSelection() {
        String sql = "SELECT id, payload FROM topn_wide WHERE v = 'same' "
                + "ORDER BY amount ASC NULLS FIRST, id DESC LIMIT 7";
        assertExactOrder(sql, baseline(sql));
        long matchingRows = java.util.stream.IntStream.range(0, ROWS)
                .filter(n -> n % 3 == 2 && n % 11 != 0).count();
        Metrics selected = metrics(sql);
        assertThat(metric(selected, "redis.rows.received")).isEqualTo(matchingRows);
        assertThat(metric(selected, "redis.local-topn.candidates")).isEqualTo(7);
        assertThat(metric(selected, "redis.local-topn.projected-hash-reads")).isEqualTo(21);
    }

    @Test
    public void testNativeNumericTopNRemainsPreferred() {
        String sql = "SELECT speed, payload FROM topn_wide ORDER BY speed DESC LIMIT 5";
        assertExactOrder(sql, baseline(sql));
        Metrics metrics = metrics(sql);
        assertThat(metric(metrics, "redis.local-topn.candidates")).isZero();
        assertThat(metric(metrics, "redis.rows.received")).isEqualTo(5);
    }

    @Test
    public void testExternalRealSpellingsUseSqlFloatTies() {
        assertUpdate("CREATE TABLE topn_real_external (r real, id integer, payload varchar)");
        try {
            var redis = server.getConnection().sync();
            redis.hset("topn_real_external:a", Map.of("r", "1.00000002", "id", "1", "payload", "SQL first"));
            redis.hset("topn_real_external:b", Map.of("r", "1.00000001", "id", "2", "payload", "Redis double first"));
            String sql = "SELECT id, payload FROM topn_real_external ORDER BY r ASC, id ASC LIMIT 1";
            assertExactOrder(sql, sql.replace("FROM topn_real_external", "FROM no_local_topn.tpch.topn_real_external"));
            assertThat(query(sql)).matches("VALUES (INTEGER '1', VARCHAR 'SQL first')");
            assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isEqualTo(1);
        } finally {
            assertUpdate("DROP TABLE topn_real_external");
        }
    }

    @Test
    public void testCandidateBudgetAndDisableKeepTrinoFallback() {
        String sql = "SELECT id FROM topn_wide ORDER BY id DESC LIMIT 1001";
        assertExactOrder(sql, baseline(sql));
        assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isZero();
        String larger = sql.replace("FROM topn_wide", "FROM larger_topn.tpch.topn_wide");
        assertExactOrder(larger, baseline(sql));
        Metrics largerMetrics = metrics(larger);
        assertThat(metric(largerMetrics, "redis.local-topn.candidates")).isEqualTo(1001);
        assertThat(metric(largerMetrics, "redis.local-topn.projected-hash-reads")).isEqualTo(1001);
        // 30 narrow source batches recover BIGINT values; 1001 winner reads require two bounded flushes.
        assertThat(metric(largerMetrics, "redis.exact-hash-read-batches")).isEqualTo(32);
        String disabled = "SELECT id FROM no_local_topn.tpch.topn_wide ORDER BY amount ASC NULLS FIRST, id DESC LIMIT 5";
        assertThat(metric(metrics(disabled), "redis.local-topn.candidates")).isZero();
        for (String order : List.of("id DESC", "amount ASC NULLS FIRST, id DESC")) {
            String fallback = "SELECT id FROM no_local_topn.tpch.topn_wide ORDER BY " + order + " LIMIT 5";
            assertExactOrder(fallback, fallback.replace("no_local_topn.tpch.", ""));
            Metrics fallbackMetrics = metrics(fallback);
            assertThat(metric(fallbackMetrics, "redis.rows.received")).isEqualTo(ROWS);
            assertThat(metric(fallbackMetrics, "redis.local-topn.candidates")).isZero();
        }
    }

    @Test
    public void testJsonOrderingRetainsExactTrinoFallbackWhenDisabled() {
        var redis = server.getConnection().sync();
        redis.ftCreate("topn_json_fallback", io.lettuce.core.search.arguments.CreateArgs.builder()
                .on(io.lettuce.core.search.arguments.CreateArgs.TargetType.JSON).withPrefix("topn_json_fallback:").build(),
                List.of(io.lettuce.core.search.arguments.NumericFieldArgs.builder().name("$.id").as("id").build()));
        RediSearchColumnTypes.write(redis, "topn_json_fallback", Map.of("id", io.trino.spi.type.BigintType.BIGINT));
        try {
            redis.jsonSet("topn_json_fallback:1", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":9007199254740992}");
            redis.jsonSet("topn_json_fallback:2", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":9007199254740993}");
            redis.jsonSet("topn_json_fallback:3", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":-1}");
            redis.jsonSet("topn_json_fallback:4", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"other\":true}");
            server.awaitIndexed("topn_json_fallback");
            String sql = "SELECT id FROM no_local_topn.tpch.topn_json_fallback ORDER BY id DESC NULLS FIRST LIMIT 2";
            assertThat(computeActual(sql).getOnlyColumn()).containsExactly(null, 9007199254740993L);
            Metrics fallbackMetrics = metrics(sql);
            assertThat(metric(fallbackMetrics, "redis.rows.received")).isEqualTo(4);
            assertThat(metric(fallbackMetrics, "redis.local-topn.candidates")).isZero();
        } finally {
            redis.ftDropindex("topn_json_fallback", true);
            redis.del(RediSearchColumnTypes.key("topn_json_fallback"));
        }
    }

    @Test
    public void testInvalidLoserValuesStillFail() {
        assertUpdate("CREATE TABLE topn_invalid (id bigint, a array(bigint), payload varchar)");
        try {
            assertUpdate("INSERT INTO topn_invalid VALUES (1, ARRAY[BIGINT '1'], 'winner')", 1);
            server.getConnection().sync().hset("topn_invalid:loser", Map.of("id", "99", "a", "not encoded", "payload", "loser"));
            assertQueryFails("SELECT payload FROM topn_invalid ORDER BY id, a LIMIT 1", ".*Invalid encoded value.*");
        } finally {
            assertUpdate("DROP TABLE topn_invalid");
        }
    }

    @Test
    public void testKeyAndRepeatedSortColumns() {
        String sql = "SELECT id, __key FROM topn_wide ORDER BY __key DESC NULLS FIRST, id ASC, id DESC LIMIT 5";
        assertExactOrder(sql, baseline(sql));
        assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isEqualTo(5);
    }

    private void assertExactOrder(String sql, String expectedSql) {
        var actual = computeActual(sql);
        var expected = computeActual(expectedSql);
        assertThat(actual.getTypes()).isEqualTo(expected.getTypes());
        assertThat(actual.getMaterializedRows().stream().map(io.trino.testing.MaterializedRow::getFields).toList())
                .containsExactlyElementsOf(expected.getMaterializedRows().stream().map(io.trino.testing.MaterializedRow::getFields).toList());
    }

    private static String baseline(String sql) { return sql.replace("FROM topn_wide", "FROM no_local_topn.tpch.topn_wide"); }

    private Metrics metrics(String sql) {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
    }

    private static long metric(Metrics metrics, String name) {
        return ((LongCount) metrics.getMetrics().get(name)).getTotal();
    }
}
