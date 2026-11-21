package com.redis.trino;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** Table size can exceed the group budget when canonical keys prove a smaller group bound. */
public class TestFiniteGroupingBounds extends AbstractTestQueryFramework {
    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.NON_SHARDED;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        redis.ftCreate("finite_bounds", CreateArgs.builder().withPrefix("finite_bounds:").build(), List.of(
                TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("flag").build(),
                TagFieldArgs.builder().name("other_flag").build(),
                TagFieldArgs.builder().name("zero").build(), TagFieldArgs.builder().name("label").build(),
                NumericFieldArgs.builder().name("amount").build()));
        redis.set(RediSearchColumnTypes.key("finite_bounds"),
                "{\"id\":\"varchar\",\"flag\":\"boolean\",\"other_flag\":\"boolean\",\"zero\":\"char(0)\",\"label\":\"varchar\",\"amount\":\"double\"}");
        server.awaitIndexed("finite_bounds");
        List<RedisFuture<?>> writes = new ArrayList<>();
        String[] flags = {"true", "TRUE", "false", "junk", null, null};
        String[] others = {"TRUE", "false", null};
        for (int i = 0; i < 1050; i++) {
            Map<String, String> row = new HashMap<>(Map.of("id", Integer.toString(i), "label", "same", "amount", "1.5"));
            if (flags[i % flags.length] != null) {
                row.put("flag", flags[i % flags.length]);
            }
            String other = others[(i / flags.length) % others.length];
            if (other != null) {
                row.put("other_flag", other);
            }
            if (i % 2 == 0) {
                row.put("zero", i % 4 == 0 ? "noncanonical" : "");
            }
            writes.add(server.getConnection().async().hset("finite_bounds:" + i, row));
        }
        assertThat(LettuceFutures.awaitAll(Duration.ofSeconds(30), writes.toArray(new RedisFuture<?>[0]))).isTrue();
        server.awaitIndexed("finite_bounds");
        // Create catalogs only after population, so their cached document count is 1050, never an empty-table count.
        var runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.aggregation-group-limit", "6"));
        try {
            for (int budget : new int[] {2, 3, 8, 9}) {
                Map<String, String> config = catalogProperties(server, budget);
                runner.createCatalog("groups_" + budget, "redisearch", config);
            }
            Map<String, String> fallback = catalogProperties(server, 6);
            fallback.put("redisearch.aggregation-pushdown.enabled", "false");
            runner.createCatalog("groups_fallback", "redisearch", fallback);
            Map<String, String> resp2 = catalogProperties(server, 3);
            resp2.put("redisearch.resp2", "true");
            runner.createCatalog("groups_3_resp2", "redisearch", resp2);
            return runner;
        }
        catch (RuntimeException | Error failure) {
            RediSearchQueryRunner.closeAllSuppress(failure, runner);
            throw failure;
        }
    }

    @Test
    void testBooleanBoundFitsThreeGroupsButNotTwo() {
        assertThat(computeActual("SHOW STATS FOR groups_2.tpch.finite_bounds").getMaterializedRows().getLast().getField(4))
                .isEqualTo(1050.0);
        String grouped = "SELECT flag, count(*) FROM %s.tpch.finite_bounds GROUP BY flag";
        assertThat(query(grouped.formatted("groups_3"))).isFullyPushedDown()
                .matches(grouped.formatted("groups_fallback"));
        assertThat(query(grouped.formatted("groups_2"))).isNotFullyPushedDown(AggregationNode.class)
                .matches(grouped.formatted("groups_fallback"));
        assertThat(sourceRows(grouped.formatted("groups_3"))).isEqualTo(3);
        assertThat(sourceRows(grouped.formatted("groups_2"))).isEqualTo(1050);
    }

    @Test
    void testCharZeroAndProductsUseTheirOwnFiniteBounds() {
        String zero = "SELECT zero, count(*) FROM %s.tpch.finite_bounds GROUP BY zero";
        assertThat(query(zero.formatted("groups_2"))).isFullyPushedDown().matches(zero.formatted("groups_fallback"));
        assertThat(sourceRows(zero.formatted("groups_2"))).isEqualTo(2);
        String product = "SELECT flag, zero, count(*) FROM %s.tpch.finite_bounds GROUP BY flag, zero";
        assertThat(query(product.formatted("redisearch"))).isFullyPushedDown().matches(product.formatted("groups_fallback"));
        assertThat(query(product.formatted("groups_3"))).isNotFullyPushedDown(AggregationNode.class)
                .matches(product.formatted("groups_fallback"));
        assertThat(sourceRows(product.formatted("redisearch"))).isEqualTo(6);
        assertThat(sourceRows(product.formatted("groups_3"))).isEqualTo(1050);
        String distinct = "SELECT flag, count(DISTINCT zero) FROM %s.tpch.finite_bounds GROUP BY flag";
        assertThat(query(distinct.formatted("redisearch"))).isFullyPushedDown()
                .matches(distinct.formatted("groups_fallback"));
        assertThat(sourceRows(distinct.formatted("redisearch"))).isEqualTo(3);
    }

    @Test
    void testTwoBooleanKeysNeedNineGroupsAndDistinctStillBoundsItsFirstStage() {
        String sql = "SELECT flag, other_flag, count(*) FROM %s.tpch.finite_bounds GROUP BY flag, other_flag";
        assertThat(query(sql.formatted("groups_9"))).isFullyPushedDown().matches(sql.formatted("groups_fallback"));
        assertThat(query(sql.formatted("groups_8"))).isNotFullyPushedDown(AggregationNode.class)
                .matches(sql.formatted("groups_fallback"));
        assertThat(sourceRows(sql.formatted("groups_9"))).isEqualTo(9);
        assertThat(sourceRows(sql.formatted("groups_8"))).isEqualTo(1050);
        // The final result has nine groups, but the DISTINCT stage also groups zero (up to 18 groups).
        String distinct = "SELECT flag, other_flag, count(DISTINCT zero) FROM %s.tpch.finite_bounds GROUP BY flag, other_flag";
        assertThat(query(distinct.formatted("groups_9"))).isNotFullyPushedDown(AggregationNode.class)
                .matches(distinct.formatted("groups_fallback"));
    }

    @Test
    void testVarcharStillNeedsAnActualProvenBound() {
        // Even though every current label happens to be identical, the type itself is not a finite proof.
        String sql = "SELECT label, count(*) FROM %s.tpch.finite_bounds GROUP BY label";
        assertThat(query(sql.formatted("groups_3"))).isNotFullyPushedDown(AggregationNode.class)
                .matches(sql.formatted("groups_fallback"));
        assertThat(sourceRows(sql.formatted("groups_3"))).isEqualTo(1050);
    }

    @Test
    void testFiniteGroupProofDoesNotBypassOtherGuards() {
        String floating = "SELECT flag, sum(amount) FROM %s.tpch.finite_bounds GROUP BY flag";
        assertThat(query(floating.formatted("groups_3_resp2"))).isNotFullyPushedDown(AggregationNode.class)
                .matches(floating.formatted("groups_fallback"));
        String residual = "SELECT flag, count(*) FROM %s.tpch.finite_bounds WHERE sin(CAST(id AS double)) > 0 GROUP BY flag";
        assertThat(query(residual.formatted("groups_3"))).matches(residual.formatted("groups_fallback"));
        // A residual filter must read every source row before Trino performs the aggregation.
        assertThat(sourceRows(residual.formatted("groups_3"))).isEqualTo(1050);
    }

    private long sourceRows(String sql) {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
        return ((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal();
    }

    private static Map<String, String> catalogProperties(RediSearchServer server, int budget) {
        Map<String, String> config = new HashMap<>(server.getConnectorProperties());
        config.put("redisearch.default-schema-name", "tpch");
        config.put("redisearch.aggregation-group-limit", Integer.toString(budget));
        return config;
    }
}
