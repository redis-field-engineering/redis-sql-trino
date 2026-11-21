package com.redis.trino;

import static io.airlift.slice.Slices.utf8Slice;

import java.util.List;
import java.util.UUID;

import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.lettuce.core.ScriptOutputType;

/**
 * Stages inserts as LIST entries, which HASH/JSON indexes cannot see, even with an empty index prefix. Only the
 * coordinator publishes them after all source scans finish, preventing INSERT SELECT/MERGE from reading their own
 * inserts through a keyset scan. Orphans expire after a worker/coordinator failure.
 */
final class RediSearchFlexWriteBuffer {
    static final String PREFIX = "__trino:staging:";
    static final String ROW_MARKER = "__trino_row_marker";
    static final JsonCodec<List<String>> ROW_CODEC = JsonCodec.listJsonCodec(String.class);
    private static final String APPEND = "redis.call('RPUSH', KEYS[1], ARGV[1]); "
            + "redis.call('EXPIRE', KEYS[1], 86400); "
            + "for i=2,#ARGV do redis.call('RPUSH', KEYS[1], ARGV[i]); end; return #ARGV";

    private final RediSearchSession session;
    private final String key = PREFIX + UUID.randomUUID();
    private boolean used;
    private long rowCount;

    RediSearchFlexWriteBuffer(RediSearchSession session) { this.session = session; }

    void append(RediSearchSession.Writer writer, List<String> rows) {
        if (!rows.isEmpty()) {
            used = true;
            writer.flush(List.of(writer.commands().eval(APPEND, ScriptOutputType.INTEGER,
                    new String[] {key}, rows.toArray(String[]::new))));
            rowCount = Math.addExact(rowCount, rows.size());
        }
    }

    List<Slice> finish() {
        return used ? List.of(utf8Slice(ROW_CODEC.toJson(List.of(key, Long.toString(rowCount))))) : List.of();
    }

    void abort() {
        if (used) { session.discardFlexStaging(key); }
    }
}
