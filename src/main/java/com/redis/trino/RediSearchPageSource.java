/*
 * MIT License
 *
 * Copyright (c) 2022, Redis Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.redis.trino;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.redis.trino.RediSearchPageSourceResultWriter.ValueWriter;

import io.airlift.log.Logger;
import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

/**
 * Reads the rows of an FT.AGGREGATE and its cursor. While Trino processes a batch, the next {@link #READS_AHEAD}
 * batches are read. Trino waits on {@link #isBlocked()} when it gets ahead of Redis.
 */
public class RediSearchPageSource implements ConnectorPageSource {

	private static final Logger log = Logger.get(RediSearchPageSource.class);

	private static final int ROWS_PER_PAGE = 1024;

	// A cursor keeps its ID from read to read, so a read can be sent before the reply to the one before arrives, and
	// Redis runs a connection's commands in order, so the replies come in the order of the batches. Reading two ahead
	// made a full scan 14% faster, but other queries on the scan's connection waited behind both batches: point
	// lookups during scans took 2.8 times as long (#115).
	static final int READS_AHEAD = 1;

	private final RediSearchSession session;
	private final RediSearchSession.Connection connection;
	private final RediSearchSession.ExactHashReader exactReader;
	private final RediSearchTableHandle table;
	private final ValueWriter[] writers;
	private final PageBuilder pageBuilder;
	private final RediSearchRowReader reader;
	private final RediSearchReadStats stats = new RediSearchReadStats();
	private Iterator<String[]> rows;
	// The cursor the batches are read from, until one exhausts it
	private Optional<Cursor> cursor;
	// The reads of the next batches, oldest first
	private final Deque<CompletableFuture<AggregationReply<String>>> reads = new ArrayDeque<>();
	private long completedBytes;
	private boolean finished;

	public RediSearchPageSource(RediSearchSession session, RediSearchTableHandle table,
			List<RediSearchColumnHandle> columns) {
		this(session, table, columns, Optional.empty());
	}

	public RediSearchPageSource(RediSearchSession session, RediSearchTableHandle table,
			List<RediSearchColumnHandle> columns, Optional<RediSearchScanPartition> partition) {
		this.session = session;
		this.connection = partition.map(value -> session.scanConnection(value.connectionSlot()))
				.orElseGet(session::scanConnection);
		this.exactReader = session.exactHashReader();
		this.table = table;
		List<Type> columnTypes = columns.stream().map(RediSearchColumnHandle::getType).toList();
		this.writers = columnTypes.stream().map(RediSearchPageSourceResultWriter::writer).toArray(ValueWriter[]::new);
		this.pageBuilder = new PageBuilder(columnTypes);
		RediSearchSession.AggregateResult first;
		try {
			first = partition.isEmpty() ? session.aggregate(connection, table, columns, exactReader, stats)
					: session.aggregate(connection, table, columns, exactReader, stats, partition);
		} catch (RuntimeException | Error e) {
			exactReader.close();
			throw e;
		}
		this.reader = first.getReader();
		start(first);
	}

	private void start(RediSearchSession.AggregateResult batch) {
		rows = batch.getRows().iterator();
		cursor = batch.getCursor();
		if (cursor.isEmpty()) {
			// Exhausted, and gone: the reads sent after this batch's fail ("Cursor not found"), and have no rows
			reads.forEach(read -> read.whenComplete((reply, failure) -> {
				if (failure != null) {
					log.debug("Ignoring the failure of a read after the cursor was exhausted: %s", failure);
				}
			}));
			reads.clear();
			return;
		}
		readAhead(cursor);
	}

	private void readAhead(Optional<Cursor> current) {
		current.ifPresent(next -> {
			while (reads.size() < READS_AHEAD) {
				reads.add(session.cursorReadAsync(connection, table, next, stats));
			}
		});
	}

	@Override
	public long getCompletedBytes() {
		return completedBytes;
	}

	@Override
	public long getReadTimeNanos() {
		return stats.readTimeNanos();
	}

	@Override
	public Metrics getMetrics() {
		return stats.metrics();
	}

	@Override
	public boolean isFinished() {
		return finished;
	}

	@Override
	public CompletableFuture<?> isBlocked() {
		CompletableFuture<AggregationReply<String>> next = reads.peek();
		if (!rows.hasNext() && next != null && !next.isDone()) {
			// Done either way: a failed read is reported by getNextSourcePage
			return next.handle((reply, failure) -> null);
		}
		return NOT_BLOCKED;
	}

	@Override
	public SourcePage getNextSourcePage() {
		verify(pageBuilder.isEmpty());
		while (pageBuilder.getPositionCount() < ROWS_PER_PAGE && !pageBuilder.isFull()) {
			if (!rows.hasNext()) {
				CompletableFuture<AggregationReply<String>> next = reads.peek();
				if (next == null) {
					finished = true;
					break;
				}
				if (!next.isDone()) {
					break;
				}
				reads.remove();
				AggregationReply<String> reply = join(next);
				// Start the next cursor read before waiting for this batch's exact hash values. Those HGETs use
				// a private connection, so their wait can overlap the cursor request without delaying its commands.
				readAhead(RediSearchSession.nextCursor(reply, cursor));
				start(session.result(exactReader, reader, cursor, reply, stats));
				continue;
			}
			String[] row = rows.next();
			pageBuilder.declarePosition();
			for (int column = 0; column < writers.length; column++) {
				BlockBuilder output = pageBuilder.getBlockBuilder(column);
				if (row[column] == null) {
					output.appendNull();
				} else {
					writers[column].write(output, row[column]);
				}
			}
		}
		if (pageBuilder.isEmpty()) {
			return null;
		}
		Page page = pageBuilder.build();
		pageBuilder.reset();
		// Trino requires completed bytes to be cumulative across pages
		completedBytes += page.getSizeInBytes();
		return SourcePage.create(page);
	}

	private static <T> T join(CompletableFuture<T> future) {
		try {
			return future.join();
		} catch (CompletionException e) {
			throwIfUnchecked(e.getCause());
			throw e;
		}
	}

	@Override
	public void close() {
		cursor.ifPresent(current -> deleteAfterReads(List.copyOf(reads),
				() -> session.cursorDeleteAsync(connection, table, current)));
		reads.clear();
		cursor = Optional.empty();
		exactReader.close();
	}

	static void deleteAfterReads(List<CompletableFuture<AggregationReply<String>>> pending, Runnable delete) {
		// Wait without blocking Trino. Even if a failed read has been removed from the queue, an active cursor
		// still needs deletion. A successful final reply means Redis has already deleted it.
		CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).whenComplete((ignored, failure) -> {
			boolean exhausted = pending.stream().filter(read -> !read.isCompletedExceptionally())
					.map(CompletableFuture::join)
					.anyMatch(reply -> reply.getCursor().filter(next -> next.getCursorId() != 0).isEmpty());
			if (!exhausted) {
				delete.run();
			}
		});
	}
}
