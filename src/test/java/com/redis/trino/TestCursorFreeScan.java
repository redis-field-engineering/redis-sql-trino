package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.SearchReply;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

class TestCursorFreeScan {
    @Test
    void keysetUsesEscapedUniqueKeysWithoutOffsetsOrCursors() {
        var table = new RediSearchTableHandle(new SchemaTableName("default", "docs"), "docs");
        var translator = new RediSearchTranslator(new RediSearchConfig());
        String key = "doc:\"a\\b";
        var page = translator.aggregatePage(table, List.of(), Optional.empty(), Optional.of(key), 37);
        assertThat(page.getCommandArguments()).containsSubsequence("FILTER", "@__key > \"doc:\\\"a\\\\b\"",
                "SORTBY", "2", "@__key", "ASC", "MAX", "37", "LIMIT", "0", "37");
        assertThat(page.getArgs().getWithCursor()).isEmpty();
        assertThatThrownBy(() -> translator.aggregatePage(table.withLimit(3), List.of(), Optional.empty(), Optional.empty(), 37))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(translator.aggregate(table, List.of(), Optional.empty()).getArgs().getWithCursor()).isPresent();
    }

    @Test
    void shortBatchStillHasContinuationAndEmptyBatchExhausts() {
        assertThat(RediSearchCursorFreePageSource.lastKey(reply("a", "b"), Optional.empty(), 37)).contains("b");
        assertThat(RediSearchCursorFreePageSource.lastKey(reply("c"), Optional.of("b"), 37)).contains("c");
        assertThat(RediSearchCursorFreePageSource.lastKey(reply(), Optional.of("c"), 37)).isEmpty();
        // Redis compares UTF-8 bytes, not Java's UTF-16 code units.
        assertThat(RediSearchCursorFreePageSource.lastKey(reply("\uE000", "😀"), Optional.empty(), 2)).contains("😀");
    }

    @Test
    void rejectsMissingDuplicateReorderedAndOversizedPages() {
        for (var bad : List.of(reply("a", "a"), reply("b", "a"), reply("a", "b", "c"))) {
            assertThatThrownBy(() -> RediSearchCursorFreePageSource.lastKey(bad, Optional.empty(), 2))
                    .isInstanceOf(TrinoException.class);
        }
        assertThatThrownBy(() -> RediSearchCursorFreePageSource.lastKey(reply("a"), Optional.of("a"), 2))
                .isInstanceOf(TrinoException.class);
        var missing = reply();
        missing.getReplies().get(0).addResult(new SearchReply.SearchResult<>());
        assertThatThrownBy(() -> RediSearchCursorFreePageSource.lastKey(missing, Optional.empty(), 2))
                .isInstanceOf(TrinoException.class).hasMessageContaining("without __key");
    }

    @Test
    void rejectsKeysThatCannotBeSafelyRepresentedAsFilterLiterals() {
        assertThatThrownBy(() -> RediSearchCursorFreePageSource.lastKey(reply("a\n"), Optional.empty(), 2))
                .isInstanceOf(TrinoException.class).hasMessageContaining("control characters");
        var invalidUtf8 = reply();
        var row = new SearchReply.SearchResult<String>();
        row.addField("__key", new byte[] {(byte) 0xC3, 0x28});
        invalidUtf8.getReplies().get(0).addResult(row);
        assertThatThrownBy(() -> RediSearchCursorFreePageSource.lastKey(invalidUtf8, Optional.empty(), 2))
                .isInstanceOf(TrinoException.class).hasMessageContaining("valid UTF-8");
    }

    private static AggregationReply<String> reply(String... keys) {
        var reply = new AggregationReply<String>();
        var shard = new SearchReply<String>();
        reply.getReplies().add(shard);
        for (String key : keys) {
            var row = new SearchReply.SearchResult<String>();
            row.addField("__key", key.getBytes(StandardCharsets.UTF_8));
            shard.addResult(row);
        }
        return reply;
    }
}
