package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/** Exact stored-string LIKE predicates independent of indexed tokenization and wildcard expansion. */
public class TestLikePatternPushdown extends AbstractTestQueryFramework {
    private static final List<String> VALUES = List.of("", "aba", "ababa", "abaaba", "abaXaba", "aba😀aba",
            "😀", "😀😀", "😀X😀", "é😀é", "a_b", "a%b", "a!b", "a\\b", "a\"b", "a'b",
            "foo", "foobar", "fooXbar", "fooXbarYbaz", "fooXbarbaz", "foobarbaz", "foofoo", "fooXfoo",
            "FOOXbar", "éfoo😀baré", "a!_b", "a\nb", "😀%😀", "_", "%", "a!", "a\\_b", "foo\0bar", "foo\0barX", "a\0b", "a\\ b", "a\\\\b");
    private static final int REPEATS = 10;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        redis.ftCreate("like_values", CreateArgs.builder().withPrefix("like_value:").build(),
                List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("v").build()));
        server.awaitIndexed("like_values");
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            for (int i = 0; i < VALUES.size(); i++) {
                String id = repeat + "_" + i;
                redis.hset("like_value:" + id, Map.of("id", id, "v", VALUES.get(i)));
            }
            redis.hset("like_value:null_" + repeat, Map.of("id", "null_" + repeat));
        }
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.cursor-count", "10"));
    }

    @Test
    void testInteriorWildcardsAndUnicodeDoNotOverlap() {
        for (String pattern : List.of("aba%aba", "😀%😀", "é%é", "foo%bar", "foo%%bar", "foo%foo", "%%%", "a\\%b", "%\\", "a\\%", "a'%b", "a\"%b")) {
            assertThat(query("SELECT id, v FROM like_values WHERE v LIKE " + literal(pattern))).isFullyPushedDown();
        }
        assertThat(query("SELECT count(*) FROM like_values WHERE v LIKE 'aba%aba'"))
                .isFullyPushedDown().matches("VALUES BIGINT '30'");
        assertThat(query("SELECT count(*) FROM like_values WHERE v LIKE '😀%😀'"))
                .isFullyPushedDown().matches("VALUES BIGINT '30'");
        assertThat(query("SELECT count(*) FROM like_values WHERE v LIKE 'foo%bar%baz'"))
                .isNotFullyPushedDown(FilterNode.class).matches("VALUES BIGINT '30'");
        assertThat(query("SELECT v FROM like_values WHERE v LIKE 'a''%b' LIMIT 1"))
                .isFullyPushedDown().matches("VALUES VARCHAR 'a''b'");
    }

    @Test
    void testExplicitEscapesAndLiteralMetacharacters() {
        for (String pattern : List.of("a!_%", "a!%%", "a!!%", "a!_%b", "a!%%b", "a!!%b",
                "a\\%b", "a\\!_%", "a\\ %", "a\\\\%b", "a\"%b", "a'%b", "a!!")) {
            assertThat(query("SELECT id, v FROM like_values WHERE v LIKE " + literal(pattern) + " ESCAPE '!'"))
                    .isFullyPushedDown();
        }
        assertThat(query("SELECT count(*) FROM like_values WHERE v LIKE 'a!_%' ESCAPE '!'"))
                .isFullyPushedDown().matches("VALUES BIGINT '10'");
        assertThat(query("SELECT id FROM like_values WHERE v LIKE 'a\\_%' ESCAPE '\\'"))
                .isFullyPushedDown();
        assertThat(query("SELECT id FROM like_values WHERE v LIKE 'aé_%b' ESCAPE 'é'"))
                .isFullyPushedDown();
        assertThat(query("SELECT id FROM like_values WHERE v LIKE '😀!%%😀' ESCAPE '!'"))
                .isFullyPushedDown();
    }

    @Test
    void testBackslashAndQuoteDomainsUseExactStoredValues() {
        for (String value : List.of("a\\b", "a\\ b", "a\\\\b", "a\\_b", "\\", "a\"b", "a'b")) {
            assertThat(query("SELECT id, v FROM like_values WHERE v = " + literal(value))).isFullyPushedDown();
            assertThat(query("SELECT id, v FROM like_values WHERE v >= " + literal(value)
                    + " AND v < " + literal(value + "z"))).isFullyPushedDown();
        }
    }

    @Test
    void testUnsupportedPatternsKeepResidualAndNullSemantics() {
        for (String pattern : List.of("%foo%bar%", "a_b", "a\nb%", "foo%bar%baz%", "foo%bar%baz", "%foo%bar", "foo%bar%", "%😀%", "%bar%")) {
            assertThat(query("SELECT id FROM like_values WHERE v LIKE " + literal(pattern)))
                    .isNotFullyPushedDown(FilterNode.class);
        }
        assertThat(query("SELECT count(*) FROM like_values WHERE v LIKE 'missing%value'"))
                .isFullyPushedDown().matches("VALUES BIGINT '0'");
        assertThat(query("SELECT id FROM like_values WHERE v LIKE 'foo%bar' OR v IS NULL"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT v, count(*) FROM like_values GROUP BY v HAVING v LIKE 'foo%bar'"))
                .isFullyPushedDown();
        assertQueryFails("SELECT id FROM like_values WHERE v LIKE 'a!x%' ESCAPE '!'", ".*Escape character.*");
        assertQueryFails("SELECT id FROM like_values WHERE v LIKE 'a!' ESCAPE '!'", ".*Escape character.*");
    }

    @Test
    void testRowsAndCursorRequestsReducedAgainstDisabledPushdown() {
        String sql = "SELECT count(*) FROM like_values WHERE v LIKE 'aba%aba'";
        QueryRunner.MaterializedResultWithPlan pushed = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        Session referenceSession = Session.builder(getSession())
                .setSystemProperty("allow_pushdown_into_connectors", "false").build();
        QueryRunner.MaterializedResultWithPlan reference = getDistributedQueryRunner().executeWithPlan(referenceSession, sql);
        assertThat(pushed.result().getMaterializedRows()).isEqualTo(reference.result().getMaterializedRows());
        assertThat(pushed.result().getOnlyValue()).isEqualTo(30L);
        Metrics pushedMetrics = metrics(pushed);
        Metrics referenceMetrics = metrics(reference);
        assertThat(metric(pushedMetrics, "redis.rows.received")).isEqualTo(1);
        assertThat(metric(referenceMetrics, "redis.rows.received")).isEqualTo((VALUES.size() + 1L) * REPEATS);
        assertThat(metric(pushedMetrics, "redis.aggregate.requests")).isEqualTo(1);
        assertThat(metric(pushedMetrics, "redis.cursor.requests")).isZero();
        assertThat(metric(referenceMetrics, "redis.cursor.requests")).isGreaterThan(0);
        String rowsSql = "SELECT id, v FROM like_values WHERE v LIKE 'aba%aba'";
        QueryRunner.MaterializedResultWithPlan pushedRows = getDistributedQueryRunner().executeWithPlan(getSession(), rowsSql);
        QueryRunner.MaterializedResultWithPlan referenceRows = getDistributedQueryRunner().executeWithPlan(referenceSession, rowsSql);
        assertThat(pushedRows.result().getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(referenceRows.result().getMaterializedRows());
        Metrics scanMetrics = metrics(pushedRows);
        Metrics referenceScanMetrics = metrics(referenceRows);
        assertThat(metric(scanMetrics, "redis.rows.received")).isEqualTo(30);
        assertThat(metric(referenceScanMetrics, "redis.rows.received")).isEqualTo((VALUES.size() + 1L) * REPEATS);
        assertThat(metric(scanMetrics, "redis.cursor.requests"))
                .isLessThan(metric(referenceScanMetrics, "redis.cursor.requests"));
    }

    private Metrics metrics(QueryRunner.MaterializedResultWithPlan result) {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
    }

    private static long metric(Metrics metrics, String name) {
        return ((LongCount) metrics.getMetrics().get(name)).getTotal();
    }

    private static String literal(String value) { return "'" + value.replace("'", "''") + "'"; }
}
