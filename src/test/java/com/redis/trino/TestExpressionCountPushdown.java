package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

public class TestExpressionCountPushdown extends AbstractTestQueryFramework {
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
        var runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(), Map.of("redisearch.cursor-count", "2"));
        try {
            runner.execute("CREATE TABLE counted_expressions (g varchar, d double, e double, i integer, b bigint)");
            runner.execute("INSERT INTO counted_expressions VALUES ('a', DOUBLE '-0.0', DOUBLE '-0.0', -2147483648, -9223372036854775808), "
                    + "('a', 2, 0, 2147483647, 9223372036854775807), ('a', NULL, 3, NULL, NULL), "
                    + "('b', 4, 2, 10, 9007199254740993), ('b', 7, NULL, NULL, NULL), (NULL, NULL, NULL, NULL, NULL)");
            return runner;
        } catch (RuntimeException | Error failure) {
            RediSearchQueryRunner.closeAllSuppress(failure, runner);
            throw failure;
        }
    }

    @Test
    void testDoubleArithmeticCountsNullsNanAndInfinity() {
        assertThat(query("SELECT count(d + e), count(d * e), count(d / e), count(-d), count(d + 1e0) FROM counted_expressions"))
                .isFullyPushedDown().matches("VALUES (BIGINT '3', BIGINT '3', BIGINT '3', BIGINT '4', BIGINT '4')");
        assertThat(query("SELECT count(d - d), count(d / d), count((d + e) * (d - e)) FROM counted_expressions"))
                .isFullyPushedDown().matches("VALUES (BIGINT '4', BIGINT '4', BIGINT '3')");
        assertThat(query("SELECT g, count(d / e), count(-d) FROM counted_expressions GROUP BY g"))
                .isFullyPushedDown().matches("VALUES (VARCHAR 'a',BIGINT '2',BIGINT '2'), (VARCHAR 'b',BIGINT '1',BIGINT '2'), (CAST(NULL AS VARCHAR),BIGINT '0',BIGINT '0')");
        assertThat(query("SELECT count(d + e), count(d / e) FROM counted_expressions WHERE g='absent'"))
                .isFullyPushedDown().matches("VALUES (BIGINT '0',BIGINT '0')");
    }

    @Test
    void testIntegerWideningAndDoubleCastCountOnlyPresence() {
        assertThat(query("SELECT count(CAST(i AS BIGINT)), count(CAST(i AS DOUBLE)), count(CAST(b AS DOUBLE) + 1e0) FROM counted_expressions"))
                .isFullyPushedDown().matches("VALUES (BIGINT '3',BIGINT '3',BIGINT '3')");
        assertThat(query("SELECT g, count(CAST(b AS DOUBLE) + 1e0) FROM counted_expressions GROUP BY g"))
                .isFullyPushedDown().matches("VALUES (VARCHAR 'a',BIGINT '2'), (VARCHAR 'b',BIGINT '1'), (CAST(NULL AS VARCHAR),BIGINT '0')");
    }

    @Test
    void testMixedAggregatesNowReturnOneServerRow() {
        String sql = "SELECT count(d + e), sum(d + e), avg(d + e), count(*) FROM counted_expressions";
        assertThat(query(sql)).isFullyPushedDown().matches("VALUES (BIGINT '3',DOUBLE '8',DOUBLE '2.6666666666666665',BIGINT '6')");
        var pushed = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        Session disabled = Session.builder(getSession()).setSystemProperty("allow_pushdown_into_connectors", "false").build();
        var reference = getDistributedQueryRunner().executeWithPlan(disabled, sql);
        assertThat(pushed.result().getMaterializedRows()).isEqualTo(reference.result().getMaterializedRows());
        Metrics pushedMetrics = metrics(pushed);
        Metrics referenceMetrics = metrics(reference);
        assertThat(metric(pushedMetrics,"redis.rows.received")).isEqualTo(1);
        assertThat(metric(referenceMetrics,"redis.rows.received")).isEqualTo(6);
        assertThat(metric(pushedMetrics,"redis.cursor.requests")).isZero();
        assertThat(metric(referenceMetrics,"redis.cursor.requests")).isPositive();
        assertThat(metric(pushedMetrics,"redis.exact-hash-reads")).isZero();
    }

    @Test
    void testErroringIntegerExpressionsAndDistinctKeepFallback() {
        assertThat(query("SELECT count(i + 1) FROM counted_expressions WHERE g='b'"))
                .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class).matches("VALUES BIGINT '1'");
        assertQueryFails("SELECT count(i + 1) FROM counted_expressions", ".*(integer addition overflow|integer overflow).*" );
        assertQueryFails("SELECT count(i / 0) FROM counted_expressions WHERE g='b'", ".*(Division by zero|division by zero).*" );
        assertThat(query("SELECT count(DISTINCT d + e) FROM counted_expressions"))
                .isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '3'");
        assertThat(query("SELECT count(d + e) FILTER(WHERE g='a') FROM counted_expressions"))
                .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class).matches("VALUES BIGINT '2'");
    }

    private Metrics metrics(QueryRunner.MaterializedResultWithPlan result) {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics).reduce(Metrics.EMPTY,Metrics::mergeWith);
    }
    private static long metric(Metrics metrics, String name) { return ((LongCount)metrics.getMetrics().get(name)).getTotal(); }
}
