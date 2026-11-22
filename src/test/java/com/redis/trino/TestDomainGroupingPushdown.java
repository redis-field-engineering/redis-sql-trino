package com.redis.trino;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** Exact source IN domains can bound GROUPBY even when the table exceeds the group budget. */
public class TestDomainGroupingPushdown extends AbstractTestQueryFramework {
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        var server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        redis.ftCreate("domain_bounds", CreateArgs.builder().withPrefix("domain_bounds:").build(), List.of(
                TagFieldArgs.builder().name("label").build(), TagFieldArgs.builder().name("other").build(),
                TagFieldArgs.builder().name("flag").build()));
        redis.set(RediSearchColumnTypes.key("domain_bounds"), "{\"label\":\"varchar\",\"other\":\"varchar\",\"flag\":\"boolean\"}");
        String[] labels = {"Wheat", "wheat", "麥", " Wheat ", null, "elsewhere"};
        List<RedisFuture<?>> writes = new ArrayList<>();
        for (int i = 0; i < 1050; i++) {
            Map<String, String> row = new HashMap<>(Map.of("other", (i / 18) % 2 == 0 ? "a" : "b"));
            if (labels[i % labels.length] != null) { row.put("label", labels[i % labels.length]); }
            if ((i / labels.length) % 3 != 2) { row.put("flag", (i / labels.length) % 3 == 0 ? "TRUE" : "false"); }
            writes.add(server.getConnection().async().hset("domain_bounds:" + i, row));
        }
        assertThat(LettuceFutures.awaitAll(Duration.ofSeconds(30), writes.toArray(new RedisFuture<?>[0]))).isTrue();
        server.awaitIndexed("domain_bounds");
        redis.ftCreate("domain_json", CreateArgs.builder().on(CreateArgs.TargetType.JSON).withPrefix("domain_json:").build(),
                List.of(TagFieldArgs.builder().name("$.label[*]").as("label").build()));
        redis.set(RediSearchColumnTypes.key("domain_json"), "{\"label\":\"varchar\"}");
        for (int i = 0; i < 10; i++) {
            redis.jsonSet("domain_json:" + i, JsonPath.ROOT_PATH, "{\"label\":[\"outside\",\"Wheat\"]}");
        }
        server.awaitIndexed("domain_json");
        var runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.aggregation-group-limit", "6"));
        try {
            for (int budget : new int[] {2, 3, 4}) { runner.createCatalog("domain_" + budget, "redisearch", properties(server, budget)); }
            var fallback = properties(server, 6);
            fallback.put("redisearch.aggregation-pushdown.enabled", "false");
            runner.createCatalog("domain_fallback", "redisearch", fallback);
            return runner;
        } catch (RuntimeException | Error failure) {
            RediSearchQueryRunner.closeAllSuppress(failure, runner);
            throw failure;
        }
    }

    @Test
    void testExactInSetsRespectBudgetAndStoredStringEquality() {
        assertThat(computeActual("SHOW STATS FOR domain_2.tpch.domain_bounds").getMaterializedRows().getLast().getField(4)).isEqualTo(1050.0);
        String two = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat') GROUP BY label";
        assertThat(query(two.formatted("domain_2"))).isFullyPushedDown().matches(two.formatted("domain_fallback"));
        assertThat(sourceRows(two.formatted("domain_2"))).isEqualTo(2);
        String three = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat','麥') GROUP BY label";
        assertThat(query(three.formatted("domain_3"))).isFullyPushedDown().matches(three.formatted("domain_fallback"));
        assertThat(query(three.formatted("domain_2"))).isNotFullyPushedDown(AggregationNode.class).matches(three.formatted("domain_fallback"));
        assertThat(sourceRows(three.formatted("domain_3"))).isEqualTo(3);
        assertThat(sourceRows(three.formatted("domain_2"))).isEqualTo(525);
        String inNull = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat',NULL) GROUP BY label";
        assertThat(query(inNull.formatted("domain_2"))).isFullyPushedDown().matches(inNull.formatted("domain_fallback"));
    }

    @Test
    void testProductsAndDistinctBoundAllFirstStageKeys() {
        String product = "SELECT label,flag,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat') GROUP BY label,flag";
        assertThat(query(product.formatted("redisearch"))).isFullyPushedDown().matches(product.formatted("domain_fallback"));
        assertThat(sourceRows(product.formatted("redisearch"))).isEqualTo(6);
        assertThat(query(product.formatted("domain_4"))).isNotFullyPushedDown(AggregationNode.class).matches(product.formatted("domain_fallback"));
        String distinct = "SELECT label,count(DISTINCT flag) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat') GROUP BY label";
        assertThat(query(distinct.formatted("redisearch"))).isFullyPushedDown().matches(distinct.formatted("domain_fallback"));
        assertThat(sourceRows(distinct.formatted("redisearch"))).isEqualTo(2);
        assertThat(query(distinct.formatted("domain_2"))).isNotFullyPushedDown(AggregationNode.class).matches(distinct.formatted("domain_fallback"));
        String both = "SELECT label,other,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat') AND other IN ('a','b') GROUP BY label,other";
        assertThat(query(both.formatted("domain_4"))).isFullyPushedDown().matches(both.formatted("domain_fallback"));
        assertThat(query(both.formatted("domain_3"))).isNotFullyPushedDown(AggregationNode.class).matches(both.formatted("domain_fallback"));
        String conjunct = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat') AND other='a' GROUP BY label";
        assertThat(query(conjunct.formatted("domain_2"))).isFullyPushedDown().matches(conjunct.formatted("domain_fallback"));
        assertThat(sourceRows(conjunct.formatted("domain_2"))).isEqualTo(2);
        String nonKey = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE other='a' GROUP BY label";
        assertThat(query(nonKey.formatted("domain_2"))).isNotFullyPushedDown(AggregationNode.class).matches(nonKey.formatted("domain_fallback"));
    }

    @Test
    void testNullableFilterOnlyDomainsAndNonDiscretePredicatesRemainConservative() {
        String nullable = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat','wheat') OR label IS NULL GROUP BY label";
        // Nullable HASH predicates are enforced as FILTER strings, outside the indexed source TupleDomain.
        assertThat(query(nullable.formatted("domain_3"))).isNotFullyPushedDown(AggregationNode.class).matches(nullable.formatted("domain_fallback"));
        String spaces = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label IN ('Wheat',' Wheat ') GROUP BY label";
        assertThat(query(spaces.formatted("domain_2"))).isNotFullyPushedDown(AggregationNode.class).matches(spaces.formatted("domain_fallback"));
        String range = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label >= 'Wheat' GROUP BY label";
        assertThat(query(range.formatted("domain_3"))).isNotFullyPushedDown(AggregationNode.class).matches(range.formatted("domain_fallback"));
        String complement = "SELECT label,count(*) FROM %s.tpch.domain_bounds WHERE label NOT IN ('Wheat','wheat') GROUP BY label";
        assertThat(query(complement.formatted("domain_3"))).isNotFullyPushedDown(AggregationNode.class).matches(complement.formatted("domain_fallback"));
        String json = "SELECT label,count(*) FROM %s.tpch.domain_json WHERE label IN ('Wheat','wheat') GROUP BY label";
        assertThat(query(json.formatted("domain_2"))).isNotFullyPushedDown(AggregationNode.class).matches(json.formatted("domain_fallback"));
    }

    private long sourceRows(String sql) {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
        return ((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal();
    }

    private static Map<String, String> properties(RediSearchServer server, int budget) {
        Map<String, String> config = new HashMap<>(server.getConnectorProperties());
        config.put("redisearch.default-schema-name", "tpch");
        config.put("redisearch.aggregation-group-limit", Integer.toString(budget));
        return config;
    }
}
