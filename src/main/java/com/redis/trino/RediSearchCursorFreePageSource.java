package com.redis.trino;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.redis.trino.RediSearchErrorCode.REDISEARCH_UNEXPECTED_RESULT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.redis.trino.RediSearchPageSourceResultWriter.ValueWriter;

import io.airlift.slice.SizeOf;
import io.lettuce.core.search.AggregationReply;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

/**
 * Cursor-free Flex scans. Each bounded request sorts by the unique document key and selects keys strictly after the
 * previous page. No offsets, server cursor, result-window cap, or assumption that a short batch means exhaustion.
 * Like the ordinary reader, this does not offer a snapshot of concurrent writes.
 */
final class RediSearchCursorFreePageSource implements ConnectorPageSource {
    private static final int ROWS_PER_PAGE = 1024;

    private final RediSearchSession session;
    private final RediSearchSession.Connection connection;
    private final RediSearchSession.ExactHashReader exactReader;
    private final RediSearchTableHandle table;
    private final List<RediSearchColumnHandle> columns;
    private final Optional<RediSearchIndexInfo> indexInfo;
    private final RediSearchTranslator translator;
    private final RediSearchRowReader reader;
    private final int batchSize;
    private final PageBuilder builder;
    private final ValueWriter[] writers;
    private final RediSearchReadStats stats = new RediSearchReadStats();
    private CompletableFuture<AggregationReply<String>> pending;
    private Iterator<String[]> rows = Collections.emptyIterator();
    private Optional<String> afterKey = Optional.empty();
    private long retainedBytes;
    private long completedBytes;
    private boolean finished;

    RediSearchCursorFreePageSource(RediSearchSession session, RediSearchTableHandle table,
            List<RediSearchColumnHandle> columns) {
        this.session = session;
        this.table = table;
        this.columns = List.copyOf(columns);
        this.connection = session.scanConnection();
        this.batchSize = session.getConfig().getFlexBatchSize();
        this.translator = new RediSearchTranslator(session.getConfig());
        this.indexInfo = session.cursorFreeIndexInfo(connection, table, stats);
        var types = columns.stream().map(RediSearchColumnHandle::getType).toList();
        this.builder = new PageBuilder(types);
        this.writers = types.stream().map(RediSearchPageSourceResultWriter::writer).toArray(ValueWriter[]::new);
        this.reader = translator.aggregatePage(table, columns, indexInfo, Optional.empty(), batchSize).getReader();
        this.exactReader = session.exactHashReader();
        try {
            pending = read();
        } catch (RuntimeException | Error failure) {
            exactReader.close();
            throw failure;
        }
    }

    private CompletableFuture<AggregationReply<String>> read() {
        return session.aggregateAsync(connection,
                translator.aggregatePage(table, columns, indexInfo, afterKey, batchSize), stats);
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) {
            return null;
        }
        try {
            while (builder.getPositionCount() < ROWS_PER_PAGE && !builder.isFull()) {
                if (!rows.hasNext()) {
                    if (pending == null) {
                        finished = true;
                        retainedBytes = 0;
                        break;
                    }
                    if (!pending.isDone()) {
                        break;
                    }
                    var reply = pending.join();
                    pending = null;
                    RediSearchQueryErrors.verifyComplete(reply);
                    if (reply.getCursor().filter(cursor -> cursor.getCursorId() != 0).isPresent()) {
                        throw new TrinoException(REDISEARCH_UNEXPECTED_RESULT, "Cursor-free scan returned a server cursor");
                    }
                    Optional<String> last = lastKey(reply, afterKey, batchSize);
                    // Only an empty server batch ends the scan. A short batch can reflect a server result limit.
                    if (last.isPresent()) {
                        afterKey = last;
                        pending = read();
                    }
                    var batch = session.result(exactReader, reader, Optional.empty(), reply, stats).getRows();
                    retainedBytes = SizeOf.estimatedSizeOf(batch, row -> {
                        long size = SizeOf.sizeOf(row);
                        for (String value : row) { size += SizeOf.estimatedSizeOf(value); }
                        return size;
                    });
                    rows = batch.iterator();
                    continue;
                }
                String[] row = rows.next();
                builder.declarePosition();
                for (int i = 0; i < writers.length; i++) {
                    if (row[i] == null) {
                        builder.getBlockBuilder(i).appendNull();
                    } else {
                        writers[i].write(builder.getBlockBuilder(i), row[i]);
                    }
                }
            }
            if (builder.isEmpty()) {
                return null;
            }
            var page = builder.build();
            builder.reset();
            completedBytes += page.getSizeInBytes();
            return SourcePage.create(page);
        } catch (CompletionException failure) {
            close();
            throwIfUnchecked(failure.getCause());
            throw failure;
        } catch (RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    /** Validate progress before scheduling another request or exposing rows. */
    static Optional<String> lastKey(AggregationReply<String> reply, Optional<String> after, int batchSize) {
        Optional<String> last = after;
        int count = 0;
        for (var shard : reply.getReplies()) {
            for (var row : shard.getResults()) {
                var field = row.getFields().get(RediSearchBuiltinField.KEY.getName());
                if (field == null || field.isNull()) {
                    throw new TrinoException(REDISEARCH_UNEXPECTED_RESULT, "Cursor-free scan returned a row without __key");
                }
                String key = field.asString();
                if (!Arrays.equals(field.asBytes(), key.getBytes(StandardCharsets.UTF_8))) {
                    throw new TrinoException(NOT_SUPPORTED, "Cursor-free Flex scans require valid UTF-8 document keys");
                }
                verifyKey(key);
                if (++count > batchSize || last.filter(previous -> compareKeys(previous, key) >= 0).isPresent()) {
                    throw new TrinoException(REDISEARCH_UNEXPECTED_RESULT,
                            "Cursor-free scan did not return a bounded batch of strictly increasing document keys");
                }
                last = Optional.of(key);
            }
        }
        return count == 0 ? Optional.empty() : last;
    }

    static void verifyKey(String key) {
        // The connector uses UTF-8 keys. FILTER literals cannot safely represent ASCII control characters.
        if (key.chars().anyMatch(value -> value < 32 || value == 127)) {
            throw new TrinoException(NOT_SUPPORTED, "Cursor-free Flex scans require document keys without ASCII control characters");
        }
    }

    private static int compareKeys(String left, String right) {
        return Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public CompletableFuture<?> isBlocked() {
        return !rows.hasNext() && pending != null ? pending.handle((reply, failure) -> null) : NOT_BLOCKED;
    }

    @Override
    public boolean isFinished() { return finished; }
    @Override
    public long getCompletedBytes() { return completedBytes; }
    @Override
    public long getReadTimeNanos() { return stats.readTimeNanos(); }
    @Override
    public Metrics getMetrics() { return stats.metrics(); }
    @Override
    public long getMemoryUsage() {
        long bytes = retainedBytes + builder.getRetainedSizeInBytes();
        if (pending != null && pending.isDone() && !pending.isCompletedExceptionally()) {
            for (var shard : pending.join().getReplies()) {
                for (var row : shard.getResults()) {
                    for (var field : row.getFields().entrySet()) {
                        bytes += SizeOf.estimatedSizeOf(field.getKey());
                        if (field.getValue() != null && !field.getValue().isNull()) {
                            bytes += SizeOf.estimatedSizeOf(field.getValue().asString());
                        }
                    }
                }
            }
        }
        return bytes;
    }

    @Override
    public void close() {
        finished = true;
        if (pending != null) {
            // Cancels local consumption. An already-sent read has no server state to release or replay.
            pending.cancel(false);
            pending = null;
        }
        exactReader.close();
        retainedBytes = 0;
        rows = Collections.emptyIterator();
        builder.reset();
    }
}
