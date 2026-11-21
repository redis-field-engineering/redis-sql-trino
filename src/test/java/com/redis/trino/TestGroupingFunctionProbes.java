package com.redis.trino;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.QueryDialects;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.spi.type.CharType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static org.assertj.core.api.Assertions.assertThat;

/** Real Query Engine expression behavior, compared to the exact CHAR reader used by the connector. */
class TestGroupingFunctionProbes {
    @Test
    void testUtf8WidthsTruncationAndTrailingSpaces() {
        try (RediSearchServer server = new RediSearchServer()) {
            var redis = server.getConnection().sync();
            redis.ftCreate("char_probe", CreateArgs.builder().withPrefix("char_probe:").build(),
                    List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("c").build()));
            server.awaitIndexed("char_probe");
            List<String> values = List.of("", " ", "  ", "a", "a ", "a  ", " ab", "abc", "é", "é ", "éx", "😀",
                    "😀 ", "😀a", "😀ab", "\u007F", "\u0080", "\u07FF", "\u0800", "\uFFFF", "\uD800\uDC00", "\uDBFF\uDFFF",
                    "e\u0301 ", "x\u00A0 ", "x  y", " \u00A0", "😀é中z  ", "e\u0301more");
            for (int i = 0; i < values.size(); i++) {
                redis.hset("char_probe:" + i, Map.of("id", Integer.toString(i), "c", values.get(i)));
            }
            redis.hset("char_probe:missing", Map.of("id", "missing"));
            for (int length = 0; length <= 8; length++) {
                CharType type = CharType.createCharType(length);
                var plan = RediSearchGroupingNormalizer.plan(List.of(new RediSearchAggregationTerm("c", type)), Set.of("id", "c"));
                AggregateArgs.Builder args = AggregateArgs.builder().load("@id").load("@c").dialect(QueryDialects.DIALECT2);
                plan.beforeExpressions().forEach((name, expression) -> args.apply(expression, name));
                plan.afterExpressions().forEach((name, expression) -> args.apply(expression, name));
                var reply = redis.ftAggregate("char_probe", "*", args.build());
                RediSearchQueryErrors.verifyComplete(reply);
                Map<String, String> actual = new HashMap<>();
                for (var shard : reply.getReplies()) {
                    for (var row : shard.getResults()) {
                        var value = row.getFields().get("c");
                        actual.put(row.getFields().get("id").asString(), value == null || value.isNull() ? null : value.asString());
                    }
                }
                assertThat(actual).hasSize(values.size() + 1);
                assertThat(actual.get("missing")).isNull();
                for (int i = 0; i < values.size(); i++) {
                    assertThat(actual.get(Integer.toString(i))).as("CHAR(%s) of [%s]", length, values.get(i))
                            .isEqualTo(truncateToLengthAndTrimSpaces(utf8Slice(values.get(i)), type).toStringUtf8());
                }
            }
        }
    }
}
