package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.Session;
import io.trino.spi.type.IntegerType;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

/** HASH/JSON predicate parity against Trino evaluation over the identical unfiltered source. */
public class TestPredicateAcceptance extends AbstractTestQueryFramework {
    private static final List<String> VALUES = Arrays.asList(null, "", "Wheat", "wheat", "a,b", "a;b", " a", "a ",
            "a\tb", "a\nb", "a\0b", "Café", "😀", "\uE000", "𐀀", "a_b", "a%b", "a!b", "a\\b", "say \"hi\"", "a'b",
            "fooXbar", "fooXbarYbaz", "the");
    private static final int REPEATS = 3;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        for (boolean json : List.of(false, true)) {
            String table = json ? "predicate_json" : "predicate_hash";
            var args = CreateArgs.builder().withPrefix(table + ":").skipInitialScan();
            if (json) { args.on(CreateArgs.TargetType.JSON); }
            String path = json ? "$." : "";
            redis.ftCreate(table, args.build(), List.of(
                    NumericFieldArgs.builder().name(path + "id").as("id").build(),
                    NumericFieldArgs.builder().name(path + "n").as("n").build(),
                    TagFieldArgs.builder().name(path + "v").as("v").separator(";").build(),
                    TextFieldArgs.builder().name(path + "t").as("t").build()));
            RediSearchColumnTypes.write(redis, table, Map.of("id", IntegerType.INTEGER));
            List<String> keys = server.keysOnShards(table + ":", IntStream.range(0, VALUES.size() * REPEATS)
                    .map(i -> i % 2).toArray());
            for (int i = 0; i < keys.size(); i++) {
                String value = VALUES.get(i % VALUES.size());
                Map<String, Object> document = new HashMap<>();
                document.put("id", i);
                if (i % 4 != 0) { document.put("n", i % 9); }
                if (value != null) { document.put("v", value); document.put("t", value); }
                // Exercise both absent and explicit JSON null; SQL reads both as NULL.
                else if (json && i >= VALUES.size()) { document.put("v", null); document.put("t", null); }
                if (json) {
                    redis.jsonSet(keys.get(i), JsonPath.ROOT_PATH, JsonCodec.jsonCodec(Object.class).toJson(document));
                } else {
                    Map<String, String> fields = new HashMap<>();
                    document.forEach((name, entry) -> fields.put(name, entry.toString()));
                    redis.hset(keys.get(i), fields);
                }
            }
            server.awaitIndexed(table);
        }
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.cursor-count", "7"));
    }

    @Test
    public void testAbsentEmptyCaseSeparatorsWhitespaceControlsAndUnicode() {
        List<String> predicates = new ArrayList<>(List.of("v IS NULL", "v IS NOT NULL", "v = ''", "v = 'wheat'",
                "v = 'a,b'", "v = 'a;b'", "v = ' a'", "v = 'a '", "v = " + literal("a\tb"),
                "v = " + literal("a\nb"), "v = " + literal("a\0b"), "v = " + literal("a\\b"),
                "v = " + literal("say \"hi\""), "v = " + literal("a'b"), "v >= 'Café' AND v < 'the'",
                "v > '\uE000'", "t = 'the'", "t = 'Wheat'"));
        for (String table : tables()) {
            for (String predicate : predicates) {
                assertReference("SELECT id, v, n FROM " + table + " WHERE " + predicate + " ORDER BY id", true);
            }
            assertThat(computeActual("SELECT count(*) FROM " + table + " WHERE v IS NULL").getOnlyValue()).isEqualTo(3L);
            assertThat(computeActual("SELECT count(*) FROM " + table + " WHERE v = ''").getOnlyValue()).isEqualTo(3L);
        }
    }

    @Test
    public void testNullableCompoundOrDoesNotTurnUnknownIntoTrueOrLoseNullRows() {
        for (String table : tables()) {
            for (String predicate : List.of("v = '' OR v IS NULL", "v < 'b' OR v IS NULL",
                    "(v = 'wheat' AND n > 2) OR (v IS NULL AND n IS NULL)",
                    "(v LIKE 'a!_%' ESCAPE '!' OR v IS NULL) AND (n < 5 OR n IS NULL)",
                    "NOT (v = 'wheat' OR n > 6)", "(v IS NULL OR n IS NULL) AND (v <> '' OR n = 1)")) {
                assertReference("SELECT id, v, n FROM " + table + " WHERE " + predicate + " ORDER BY id", true);
                assertReference("SELECT v, count(*), sum(n) FROM " + table + " WHERE " + predicate + " GROUP BY v", false);
            }
        }
    }

    @Test
    public void testEscapedAndGeneralLikePatternsPreserveSqlSemantics() {
        for (String table : tables()) {
            for (String predicate : List.of("v LIKE 'a!_%' ESCAPE '!'", "v LIKE 'a!%%' ESCAPE '!'",
                    "v LIKE 'a!!%' ESCAPE '!'", "v LIKE 'a\\%'", "v LIKE '😀%'", "v LIKE '%é'",
                    "v LIKE 'a_b'", "v LIKE 'foo%bar%baz'", "v LIKE " + literal("a\nb%"))) {
                assertReference("SELECT id, v FROM " + table + " WHERE " + predicate + " ORDER BY id", true);
            }
        }
    }

    @Test
    public void testRequiredResidualStaysBeforeLimitAndAggregation() {
        for (String table : tables()) {
            String predicate = "v LIKE 'foo%bar%baz'";
            assertThat(query("SELECT id FROM " + table + " WHERE " + predicate))
                    .isNotFullyPushedDown(FilterNode.class);
            // Each shard has many nonmatches; pre-limiting the source could hide these three matches.
            String limited = "SELECT count(*) FROM (SELECT id FROM " + table + " WHERE " + predicate + " LIMIT 5)";
            assertReference(limited, false);
            assertThat(computeActual(limited).getOnlyValue()).isEqualTo(3L);
            assertReference("SELECT id, v FROM " + table + " WHERE " + predicate + " ORDER BY id DESC LIMIT 2", true);
            String aggregate = "SELECT count(*), count(n), sum(n) FROM " + table + " WHERE " + predicate;
            assertReference(aggregate, false);
            assertThat(computeActual(aggregate).getMaterializedRows().getFirst().getField(0)).isEqualTo(3L);
            assertReference("SELECT v, count(*), sum(n) FROM " + table + " WHERE " + predicate + " GROUP BY v", false);
        }
    }

    private void assertReference(String sql, boolean ordered) {
        MaterializedResult actual = computeActual(sql);
        Session reference = Session.builder(getSession()).setSystemProperty("allow_pushdown_into_connectors", "false").build();
        MaterializedResult expected = getQueryRunner().execute(reference, sql);
        assertThat(actual.getTypes()).as(sql).isEqualTo(expected.getTypes());
        if (ordered) { assertThat(actual.getMaterializedRows()).as(sql).containsExactlyElementsOf(expected.getMaterializedRows()); }
        else { assertThat(actual.getMaterializedRows()).as(sql).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows()); }
    }

    private static List<String> tables() { return List.of("predicate_hash", "predicate_json"); }

    private static String literal(String value) {
        if (value.chars().noneMatch(Character::isISOControl)) { return "'" + value.replace("'", "''") + "'"; }
        StringBuilder escaped = new StringBuilder("U&'");
        for (char c : value.toCharArray()) {
            if (Character.isISOControl(c)) { escaped.append(String.format("\\%04X", (int) c)); }
            else if (c == '\\') { escaped.append("\\\\"); }
            else if (c == '\'') { escaped.append("''"); }
            else { escaped.append(c); }
        }
        return escaped.append("'").toString();
    }
}
