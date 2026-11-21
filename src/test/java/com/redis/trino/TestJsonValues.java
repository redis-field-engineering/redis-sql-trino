package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class TestJsonValues {
    @Test
    void testSinglePathUsesScanScalarSemantics() {
        assertThat(RediSearchJsonValues.decode("[9007199254740993]", List.of("$.id")))
                .containsEntry("$.id", "9007199254740993");
        assertThat(RediSearchJsonValues.decode("[0.1234567890123456]", List.of("$.n")))
                .containsEntry("$.n", "0.1234567890123456");
        assertThat(RediSearchJsonValues.decode("[true]", List.of("$.b"))).containsEntry("$.b", "1");
        assertThat(RediSearchJsonValues.decode("[false]", List.of("$.b"))).containsEntry("$.b", "0");
        assertThat(RediSearchJsonValues.decode("[null]", List.of("$.n"))).containsEntry("$.n", null);
        assertThat(RediSearchJsonValues.decode("[]", List.of("$.n"))).containsEntry("$.n", null);
        assertThat(RediSearchJsonValues.decode(null, List.of("$.n"))).isEmpty();
        assertThat(RediSearchJsonValues.decode("[[\"a\",null]]", List.of("$.a"))).containsEntry("$.a", "[\"a\",null]");
    }

    @Test
    void testMultiPathPreservesNumbersUnicodeAndWholeDocument() {
        String root = "{\"n\":9007199254740993,\"s\":\"𐀀\\n\"}";
        var values = RediSearchJsonValues.decode("{\"$.n\":[9007199254740993],\"$.s\":[\"𐀀\\n\"],\"$\":[" + root + "],\"$.missing\":[]}",
                List.of("$.n", "$.s", "$", "$.missing"));
        assertThat(values).containsEntry("$.n", "9007199254740993").containsEntry("$.s", "𐀀\n")
                .containsEntry("$", root).containsEntry("$.missing", null);
    }

    @Test
    void testPathEligibilityPreservesAliasesAndRejectsMultivaluedPaths() {
        var info = new RediSearchIndexInfo(Optional.of(RediSearchIndexInfo.KeyType.JSON), List.of("doc:"), List.of(
                new RediSearchIndexInfo.Field("rank", "$.nested.rank", RediSearchFieldType.NUMERIC, Optional.empty()),
                new RediSearchIndexInfo.Field("wild", "$.tags[*]", RediSearchFieldType.TAG, Optional.empty())), false, 1, false);
        assertThat(RediSearchJsonValues.path(info, "rank")).contains("$.nested.rank");
        assertThat(RediSearchJsonValues.path(info, "$")).contains("$");
        assertThat(RediSearchJsonValues.path(info, "wild")).isEmpty();
        assertThat(RediSearchJsonValues.path(info, "unmapped")).isEmpty();
    }

    @Test
    void testMalformedSinglePathRepliesFailClosed() {
        for (String raw : List.of("", "1", "null", "{}", "[1", "[null", "[", "[1] false", "[] {}", "[{}] []")) {
            assertThatThrownBy(() -> RediSearchJsonValues.decode(raw, List.of("$.n")))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void testMalformedMultiPathRepliesFailClosed() {
        for (String raw : List.of("[]", "{}", "{\"$.n\":[1]}", "{} false", "{\"$.n\":1}", "{\"$.other\":[1]}", "{\"$.n\":[1],\"$.n\":[2]}", "{")) {
            assertThatThrownBy(() -> RediSearchJsonValues.decode(raw, List.of("$.n", "$.m")))
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
