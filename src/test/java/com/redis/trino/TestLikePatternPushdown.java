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
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
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

    @Test
    void testComplexPercentPrefilterPreservesNullUnicodeAndOrderWhileReducingRows() {
        var redis = server.getConnection().sync();
        redis.ftCreate("complex_like", CreateArgs.builder().withPrefix("complex_like:").skipInitialScan().build(), List.of(
                TagFieldArgs.builder().name("id").build(),
                io.lettuce.core.search.arguments.TextFieldArgs.builder().name("raw_value").as("v").build(),
                TagFieldArgs.builder().name("payload").build()));
        try {
            for (int i = 0; i < 1000; i++) {
                String value = switch (i) {
                    case 0 -> "literal%XmiddleYtailZend";
                    case 1 -> "literal_XmiddleYtailZend";
                    case 2 -> "literal!XmiddleYtailZend";
                    case 3 -> "quote\"XmiddleYtailZend";
                    case 4 -> "slash\\XmiddleYtailZend";
                    case 5 -> "";
                    default -> i < 800 ? "other" + i : i < 900 ? "pre" + "x".repeat(20) + "wrong" : "preend";
                };
                redis.hset("complex_like:" + i, Map.of("id", Integer.toString(i), "raw_value", value, "payload", "p".repeat(2048) + i));
            }
            List<String> values = List.of("preXmiddleYtailZend", "pre\0middle\0tail\0end", "pre😀middle雪tailéend", "preXtailYmiddleZend");
            int id = 1000;
            for (String value : values) {
                for (int repeat = 0; repeat < 10; repeat++) {
                    String key = Integer.toString(id++);
                    redis.hset("complex_like:" + key, Map.of("id", key, "raw_value", value, "payload", "p".repeat(2048) + key));
                }
            }
            for (int repeat = 0; repeat < 10; repeat++) {
                String key = Integer.toString(id++);
                redis.hset("complex_like:" + key, Map.of("id", key, "payload", "p".repeat(2048) + key));
            }
            server.awaitIndexed("complex_like");
            Session reference = Session.builder(getSession()).setSystemProperty("allow_pushdown_into_connectors", "false").build();
            for (String pattern : List.of("pre%middle%tail%end", "pre%middle%tail%", "%middle%tail%end", "%pre%middle%tail%end%",
                    "pre%tail%middle%end", "pre%%middle%%%tail%%end", "%middle%tail%", "pre%😀%middle%雪%tail%é%end", "quote\"%middle%tail%end", "slash\\%middle%tail%end",
                    "literal!%%middle%tail%end", "literal!_%middle%tail%end", "literal!!%middle%tail%end", "literalé%%middle%tail%end")) {
                String projection = pattern.equals("pre%middle%tail%end") ? "id, payload" : "id, v";
                String sql = "SELECT " + projection + " FROM complex_like WHERE v LIKE " + literal(pattern);
                if (pattern.contains("!")) { sql += " ESCAPE '!'"; }
                if (pattern.startsWith("literalé")) { sql += " ESCAPE 'é'"; }
                var pushed = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
                var baseline = getDistributedQueryRunner().executeWithPlan(reference, sql);
                assertThat(pushed.result().getMaterializedRows()).as(pattern)
                        .containsExactlyInAnyOrderElementsOf(baseline.result().getMaterializedRows());
                assertThat(query(sql)).isNotFullyPushedDown(FilterNode.class);
                if (pattern.equals("pre%middle%tail%end")) {
                    assertThat(pushed.result().getMaterializedRows()).hasSize(30);
                    assertThat(metric(metrics(pushed), "redis.rows.received")).isEqualTo(40);
                    assertThat(metric(metrics(baseline), "redis.rows.received")).isEqualTo(1050);
                    assertThat(metric(metrics(pushed), "redis.cursor.requests"))
                            .isLessThan(metric(metrics(baseline), "redis.cursor.requests"));
                    System.out.println("COMPLEX_LIKE_PREFILTER deployment=" + deployment() + " source=1050 candidates=40 matches=30");
                }
            }
        } finally { assertUpdate("DROP TABLE complex_like"); }
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
