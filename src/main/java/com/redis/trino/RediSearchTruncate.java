package com.redis.trino;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

/** Enumerates indexed keys before deleting any of them; index and declared schema stay intact. */
final class RediSearchTruncate {

    private RediSearchTruncate() {}

    static void truncate(RediSearchSession session, RediSearchTableHandle table) {
        Path spool = null;
        try {
            spool = Files.createTempFile("redis-trino-truncate-", ".keys");
            long keys = enumerate(session, table, spool);
            try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(spool)));
                    RediSearchSession.Writer writer = session.openWriter()) {
                while (keys > 0) {
                    checkInterrupted();
                    List<RedisFuture<?>> writes = new ArrayList<>();
                    int count = (int) Math.min(keys, 1000);
                    for (int i = 0; i < count; i++) {
                        int length = input.readInt();
                        byte[] bytes = input.readNBytes(length);
                        if (bytes.length != length) {
                            throw new IOException("Truncated key spool");
                        }
                        // Single-key commands work across slots; UNLINK reclaims large values off the Redis thread.
                        writes.add(writer.commands().unlink(new String(bytes, StandardCharsets.UTF_8)));
                    }
                    writer.flush(writes);
                    keys -= count;
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Could not spool TRUNCATE keys", e);
        }
        finally {
            if (spool != null) {
                try {
                    Files.deleteIfExists(spool);
                }
                catch (IOException e) {
                    io.airlift.log.Logger.get(RediSearchTruncate.class).warn(e, "Could not remove TRUNCATE key spool %s", spool);
                }
            }
        }
    }

    private static long enumerate(RediSearchSession session, RediSearchTableHandle table, Path spool) throws IOException {
        if (session.getConfig().isFlexEnabled()) {
            return enumerateCursorFree(session, table, spool);
        }
        RediSearchSession.Connection connection = session.scanConnection();
        RediSearchReadStats stats = new RediSearchReadStats();
        Optional<Cursor> cursor = Optional.empty();
        long count = 0;
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(spool)));
                RediSearchSession.ExactHashReader reader = session.exactHashReader()) {
            RediSearchSession.AggregateResult batch = session.aggregate(connection, table,
                    List.of(RediSearchBuiltinField.KEY.getColumnHandle()), reader, stats);
            cursor = batch.getCursor();
            while (true) {
                checkInterrupted();
                for (String[] row : batch.getRows()) {
                    byte[] key = row[0].getBytes(StandardCharsets.UTF_8);
                    output.writeInt(key.length);
                    output.write(key);
                    count++;
                }
                if (cursor.isEmpty()) {
                    return count;
                }
                var reply = session.cursorReadAsync(connection, table, cursor.orElseThrow(), stats).join();
                // Capture the current cursor before conversion, so a conversion failure still cleans it up.
                Optional<Cursor> previous = cursor;
                cursor = RediSearchSession.nextCursor(reply, previous);
                batch = session.result(reader, batch.getReader(), previous, reply, stats);
            }
        }
        finally {
            cursor.ifPresent(current -> session.cursorDeleteAsync(connection, table, current));
        }
    }

    private static long enumerateCursorFree(RediSearchSession session, RediSearchTableHandle table, Path spool) throws IOException {
        var connection = session.scanConnection();
        var stats = new RediSearchReadStats();
        var info = session.cursorFreeIndexInfo(connection, table, stats);
        var translator = new RediSearchTranslator(session.getConfig());
        Optional<String> after = Optional.empty();
        long count = 0;
        try (var output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(spool)))) {
            while (true) {
                checkInterrupted();
                var aggregation = translator.aggregatePage(table, List.of(RediSearchBuiltinField.KEY.getColumnHandle()),
                        info, after, session.getConfig().getFlexBatchSize());
                var reply = session.aggregateAsync(connection, aggregation, stats).join();
                RediSearchQueryErrors.verifyComplete(reply);
                var next = RediSearchCursorFreePageSource.lastKey(reply, after, session.getConfig().getFlexBatchSize());
                if (next.isEmpty()) { return count; }
                for (var shard : reply.getReplies()) {
                    for (var row : shard.getResults()) {
                        byte[] key = row.getFields().get(RediSearchBuiltinField.KEY.getName()).asString().getBytes(StandardCharsets.UTF_8);
                        output.writeInt(key.length);
                        output.write(key);
                        count++;
                    }
                }
                after = next;
            }
        }
    }

    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new TrinoException(StandardErrorCode.USER_CANCELED, "TRUNCATE was interrupted");
        }
    }
}
