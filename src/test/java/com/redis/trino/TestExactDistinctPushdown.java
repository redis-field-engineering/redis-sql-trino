package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

public class TestExactDistinctPushdown extends AbstractTestQueryFramework {
    private RedisCommands<String, String> redis;
    private RediSearchServer server;
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        this.redis = redis;
        redis.ftCreate("distinct_values", CreateArgs.builder().withPrefix("distinct_value:").build(), List.of(
                TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("g").build(),
                TagFieldArgs.builder().name("h").build(), TagFieldArgs.builder().name("v").build()));
        server.awaitIndexed("distinct_values");
        List<Map<String, String>> rows = List.of(
                row("1", "A", "X", "red"), row("2", "A", "X", "red"), row("3", "A", "X", "blue"),
                row("4", "A", "X", null), row("5", "A", "Y", null), row("6", "A", "Y", null),
                row("7", "B", "X", "green"), row("8", "B", "X", "green"), row("9", null, "X", "red"),
                row("10", null, "X", null), row("11", "C", "Z", null), row("12", "D", "X", "red,blue"),
                row("13", "D", "X", " red "), row("14", "D", "X", ""), row("15", "D", "X", "RED"));
        List<String> keys = server.keysOnShards("distinct_value:", 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0);
        for (int i = 0; i < rows.size(); i++) { redis.hset(keys.get(i), rows.get(i)); }
        QueryRunner runner = RediSearchQueryRunner.createRediSearchQueryRunner(server);
        createCatalog(runner, "distinct_resp2", Map.of("redisearch.resp2", "true"));
        createCatalog(runner, "distinct_at_budget", Map.of("redisearch.aggregation-group-limit", "15"));
        createCatalog(runner, "distinct_over_budget", Map.of("redisearch.aggregation-group-limit", "14"));
        createCatalog(runner, "distinct_reference", Map.of("redisearch.aggregation-pushdown.enabled", "false"));
        return runner;
    }

    private void createCatalog(QueryRunner runner, String name, Map<String, String> extra) {
        Map<String, String> properties = new HashMap<>(server.getConnectorProperties());
        properties.put("redisearch.default-schema-name", "tpch");
        properties.putAll(extra);
        runner.createCatalog(name, "redisearch", properties);
    }

    private static Map<String, String> row(String id, String g, String h, String v) {
        Map<String, String> row = new HashMap<>(Map.of("id", id));
        if (g != null) { row.put("g", g); }
        if (h != null) { row.put("h", h); }
        if (v != null) { row.put("v", v); }
        return row;
    }

    @Test
    void testGlobalCountReturnsOneRowFromOneAggregateRequest() {
        String sql = "SELECT count(DISTINCT v) FROM distinct_values";
        assertThat(query(sql)).isFullyPushedDown().matches("VALUES BIGINT '7'");
        QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        assertThat(result.result().getOnlyValue()).isEqualTo(7L);
        Metrics metrics = metrics(result);
        assertThat(metric(metrics, "redis.rows.received")).isEqualTo(1);
        assertThat(metric(metrics, "redis.aggregate.requests")).isEqualTo(1);
        assertThat(metric(metrics, "redis.cursor.requests")).isZero();
        assertThat(metric(metrics, "redis.exact-hash-reads")).isZero();
        assertThat(query("SELECT count(*) FROM (SELECT DISTINCT v FROM distinct_values)"))
                .isFullyPushedDown().matches("VALUES BIGINT '8'");
    }

    @Test
    void testGroupedCountsKeepNullOnlyGroupsAndMultipleKeys() {
        assertThat(query("SELECT g, count(DISTINCT v) FROM distinct_values GROUP BY g"))
                .isFullyPushedDown().matches("VALUES (VARCHAR 'A', BIGINT '2'), (VARCHAR 'B', BIGINT '1'), "
                        + "(CAST(NULL AS varchar), BIGINT '1'), (VARCHAR 'C', BIGINT '0'), (VARCHAR 'D', BIGINT '4')");
        assertThat(query("SELECT g, h, count(DISTINCT v) FROM distinct_values GROUP BY g, h"))
                .isFullyPushedDown().matches("VALUES (VARCHAR 'A', VARCHAR 'X', BIGINT '2'), "
                        + "(VARCHAR 'A', VARCHAR 'Y', BIGINT '0'), (VARCHAR 'B', VARCHAR 'X', BIGINT '1'), "
                        + "(CAST(NULL AS varchar), VARCHAR 'X', BIGINT '1'), (VARCHAR 'C', VARCHAR 'Z', BIGINT '0'), "
                        + "(VARCHAR 'D', VARCHAR 'X', BIGINT '4')");
        assertThat(query("SELECT g, count(*), count(v) FROM (SELECT DISTINCT g, v FROM distinct_values) GROUP BY g"))
                .isFullyPushedDown().matches("VALUES (VARCHAR 'A', BIGINT '3', BIGINT '2'), (VARCHAR 'B', BIGINT '1', BIGINT '1'), "
                        + "(CAST(NULL AS varchar), BIGINT '2', BIGINT '1'), (VARCHAR 'C', BIGINT '1', BIGINT '0'), "
                        + "(VARCHAR 'D', BIGINT '4', BIGINT '4')");
    }

    @Test
    void testInputFiltersEmptyInputsAndLimitsStayInOrder() {
        assertThat(query("SELECT count(DISTINCT v) FROM distinct_values WHERE v = 'red' OR v IS NULL"))
                .isFullyPushedDown().matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(DISTINCT v) FROM distinct_values WHERE v LIKE '%red%'"))
                .isNotFullyPushedDown(FilterNode.class).matches("VALUES BIGINT '3'");
        assertThat(query("SELECT count(DISTINCT v) FROM distinct_values WHERE g = 'absent'"))
                .isFullyPushedDown().matches("VALUES BIGINT '0'");
        assertThat(query("SELECT g, count(DISTINCT v) FROM distinct_values WHERE g = 'absent' GROUP BY g"))
                .isFullyPushedDown().returnsEmptyResult();
        assertThat(query("SELECT count(*) FROM (SELECT DISTINCT v FROM distinct_values LIMIT 2)"))
                .isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '2'");
        assertThat(query("SELECT count(DISTINCT v) FROM distinct_values WHERE g = 'B' OR v = 'blue'"))
                .isNotFullyPushedDown(FilterNode.class).matches("VALUES BIGINT '2'");
    }

    @Test
    void testBigintFallbackAndGeneratedAliasCollisions() {
        assertUpdate("CREATE TABLE distinct_bigints (id bigint)");
        redis.ftCreate("distinct_count_alias", CreateArgs.builder().withPrefix("distinct_count_alias:").skipInitialScan().build(),
                List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("syntheticColumn_distinct_0").build()));
        try {
            assertUpdate("INSERT INTO distinct_bigints VALUES (9007199254740992), (9007199254740993), (9007199254740993), (NULL)", 4);
            assertThat(query("SELECT count(DISTINCT id) FROM distinct_bigints"))
                    .isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '2'");
            assertUpdate("INSERT INTO distinct_count_alias (id, syntheticColumn_distinct_0) VALUES ('1', 'a'), ('2', 'a'), ('3', 'b'), ('4', NULL)", 4);
            assertThat(query("SELECT count(DISTINCT syntheticColumn_distinct_0) FROM distinct_count_alias"))
                    .isFullyPushedDown().matches("VALUES BIGINT '2'");
        } finally {
            assertUpdate("DROP TABLE distinct_bigints");
            assertUpdate("DROP TABLE distinct_count_alias");
        }
    }

    @Test
    void testResp2KeepsExactDistinctNullAndStringSemantics() {
        assertThat(query("SELECT count(DISTINCT v) FROM distinct_resp2.tpch.distinct_values"))
                .isFullyPushedDown().matches("VALUES BIGINT '7'");
        assertThat(query("SELECT g, h, count(DISTINCT v) FROM distinct_resp2.tpch.distinct_values GROUP BY g, h"))
                .isFullyPushedDown().matches("SELECT g, h, count(DISTINCT v) FROM distinct_reference.tpch.distinct_values GROUP BY g, h");
        assertThat(query("SELECT count(DISTINCT v) FROM distinct_resp2.tpch.distinct_values WHERE g = 'absent'"))
                .isFullyPushedDown().matches("VALUES BIGINT '0'");
    }

    @Test
    void testDocumentBudgetBoundaryPreservesFallbackAndTransferWork() {
        String atBudget = "SELECT count(DISTINCT v) FROM distinct_at_budget.tpch.distinct_values";
        String overBudget = "SELECT count(DISTINCT v) FROM distinct_over_budget.tpch.distinct_values";
        assertThat(query(atBudget)).isFullyPushedDown().matches("VALUES BIGINT '7'");
        assertThat(query(overBudget)).isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '7'");
        var pushed = getDistributedQueryRunner().executeWithPlan(getSession(), atBudget);
        var fallback = getDistributedQueryRunner().executeWithPlan(getSession(), overBudget);
        assertThat(metric(metrics(pushed), "redis.rows.received")).isEqualTo(1);
        assertThat(metric(metrics(fallback), "redis.rows.received")).isEqualTo(15);
        // The conservative bound is the whole index, even when a filter matches fewer documents.
        assertThat(query(overBudget + " WHERE g = 'A'"))
                .isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '2'");
    }

    @Test
    void testHighCardinalityFallsBackWithoutLosingNullsOrLargeIntegerIdentities() {
        String index = "distinct_high_cardinality";
        redis.ftCreate(index, CreateArgs.builder().withPrefix(index + ":").skipInitialScan().build(), List.of(
                TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("g").build()));
        try {
            List<String> keys = server.keysOnShards(index + ":", java.util.stream.IntStream.range(0, 259)
                    .map(i -> i % 2).toArray());
            for (int i = 0; i < keys.size(); i++) {
                Map<String, String> row = new HashMap<>(Map.of("g", i % 2 == 0 ? "even" : "odd"));
                if (i < 257) { row.put("id", Long.toString(9007199254740992L + i)); }
                redis.hset(keys.get(i), row);
            }
            server.awaitIndexed(index);
            String table = "distinct_over_budget.tpch." + index;
            assertThat(query("SELECT count(DISTINCT id) FROM " + table))
                    .isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '257'");
            assertThat(query("SELECT g, count(DISTINCT id) FROM " + table + " GROUP BY g"))
                    .isNotFullyPushedDown(AggregationNode.class)
                    .matches("SELECT g, count(DISTINCT id) FROM distinct_reference.tpch." + index + " GROUP BY g");
        } finally {
            redis.ftDropindex(index, true);
        }
    }

    private Metrics metrics(QueryRunner.MaterializedResultWithPlan result) {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
    }

    private static long metric(Metrics metrics, String name) {
        return ((LongCount) metrics.getMetrics().get(name)).getTotal();
    }
}
