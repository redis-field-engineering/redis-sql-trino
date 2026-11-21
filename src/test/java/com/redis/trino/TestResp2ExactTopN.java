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

/** Conservatively avoid native floating SORTBY under RESP2; exact HASH values remain available through HMGET. */
public class TestResp2ExactTopN extends AbstractTestQueryFramework {
    private final double[] scores = new double[8];
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        Map<String, String> properties = new HashMap<>(Map.of("redisearch.resp2", "true",
                "redisearch.cursor-count", "2", "redisearch.query-timeout-ms", "30000"));
        QueryRunner runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(), properties);
        properties.putAll(server.getConnectorProperties());
        properties.put("redisearch.default-schema-name", "tpch");
        properties.put("redisearch.local-topn.max-candidates", "0");
        runner.createCatalog("resp2_without_local", "redisearch", properties);
        runner.execute("CREATE TABLE resp2_topn (id integer, score double, payload varchar)");
        scores[0] = 0.1234567890123456;
        for (int i = 1; i < scores.length; i++) { scores[i] = Math.nextUp(scores[i - 1]); }
        List<String> keys = server.keysOnShards("resp2_topn:", 0, 1, 0, 1, 0, 1, 0, 1);
        for (int i = 0; i < keys.size(); i++) {
            server.getConnection().sync().hset(keys.get(i), Map.of("id", Integer.toString(i + 1),
                    "score", Double.toString(scores[scores.length - i - 1]), "payload", "p".repeat(1024)));
        }
        return runner;
    }

    @Test
    public void testAdjacentDoublesUseExactLocalOrdering() {
        String sql = "SELECT id, score, payload FROM resp2_topn ORDER BY score ASC LIMIT 3";
        var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        assertThat(result.result().getMaterializedRows().stream().map(row -> row.getField(0)).toList())
                .containsExactly(8, 7, 6);
        for (int i = 0; i < 3; i++) {
            assertThat(result.result().getMaterializedRows().get(i).getField(1)).isEqualTo(scores[i]);
        }
        Metrics metrics = metrics(result);
        assertThat(metric(metrics, "redis.rows.received")).isEqualTo(8);
        assertThat(metric(metrics, "redis.local-topn.candidates")).isEqualTo(3);
        assertThat(metric(metrics, "redis.local-topn.sort-hash-reads")).isEqualTo(8);
        assertThat(metric(metrics, "redis.local-topn.projected-hash-reads")).isEqualTo(9);
        assertThat(computeActual("SELECT id FROM resp2_topn ORDER BY score DESC LIMIT 3").getOnlyColumn())
                .containsExactly(1, 2, 3);
        System.out.println("RESP2_EXACT_TOPN deployment=" + deployment() + " metrics=" + metrics.getMetrics());
    }

    @Test
    public void testDisabledLocalOrderingRetainsExactTrinoFallback() {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(),
                "SELECT id, score FROM resp2_without_local.tpch.resp2_topn ORDER BY score ASC LIMIT 3");
        assertThat(result.result().getMaterializedRows().stream().map(row -> row.getField(0)).toList())
                .containsExactly(8, 7, 6);
        for (int i = 0; i < 3; i++) {
            assertThat(result.result().getMaterializedRows().get(i).getField(1)).isEqualTo(scores[i]);
        }
        assertThat(metric(metrics(result), "redis.local-topn.candidates")).isZero();
        assertThat(metric(metrics(result), "redis.rows.received")).isEqualTo(8);
    }

    @Test
    public void testIntegerNativeOrderingStillAppliesUnderResp2() {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(),
                "SELECT id, payload FROM resp2_topn ORDER BY id DESC LIMIT 2");
        assertThat(result.result().getMaterializedRows().stream().map(row -> row.getField(0)).toList())
                .containsExactly(8, 7);
        assertThat(metric(metrics(result), "redis.local-topn.candidates")).isZero();
        assertThat(metric(metrics(result), "redis.rows.received")).isEqualTo(2);
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
