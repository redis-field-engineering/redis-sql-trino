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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.redis.trino.RediSearchPageSourceResultWriter.ValueWriter;

import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

/**
 * Reads the rows of an FT.AGGREGATE and its cursor. Each batch's next one is read while Trino processes it, and Trino
 * waits on {@link #isBlocked()} when it gets ahead of Redis.
 */
public class RediSearchPageSource implements ConnectorPageSource {

	private static final int ROWS_PER_PAGE = 1024;

	private final RediSearchSession session;
	private final RediSearchTableHandle table;
	private final ValueWriter[] writers;
	private final PageBuilder pageBuilder;
	private final RediSearchRowReader reader;
	private Iterator<String[]> rows;
	// The cursor the current batch came with, which the next one is read from
	private Optional<Cursor> cursor;
	// The read of the next batch, if the cursor has more
	private CompletableFuture<AggregationReply<String>> nextBatch;
	private long completedBytes;
	private boolean finished;

	public RediSearchPageSource(RediSearchSession session, RediSearchTableHandle table,
			List<RediSearchColumnHandle> columns) {
		this.session = session;
		this.table = table;
		List<Type> columnTypes = columns.stream().map(RediSearchColumnHandle::getType).toList();
		this.writers = columnTypes.stream().map(RediSearchPageSourceResultWriter::writer).toArray(ValueWriter[]::new);
		this.pageBuilder = new PageBuilder(columnTypes);
		RediSearchSession.AggregateResult first = session.aggregate(table, columns);
		this.reader = first.getReader();
		start(first);
	}

	private void start(RediSearchSession.AggregateResult batch) {
		rows = batch.getRows().iterator();
		cursor = batch.getCursor();
		nextBatch = cursor.map(next -> session.cursorReadAsync(table, next)).orElse(null);
	}

	@Override
	public long getCompletedBytes() {
		return completedBytes;
	}

	@Override
	public long getReadTimeNanos() {
		return 0;
	}

	@Override
	public boolean isFinished() {
		return finished;
	}

	@Override
	public CompletableFuture<?> isBlocked() {
		if (!rows.hasNext() && nextBatch != null && !nextBatch.isDone()) {
			// Done either way: a failed read is reported by getNextSourcePage
			return nextBatch.handle((reply, failure) -> null);
		}
		return NOT_BLOCKED;
	}

	@Override
	public SourcePage getNextSourcePage() {
		verify(pageBuilder.isEmpty());
		while (pageBuilder.getPositionCount() < ROWS_PER_PAGE && !pageBuilder.isFull()) {
			if (!rows.hasNext()) {
				if (nextBatch == null) {
					finished = true;
					break;
				}
				if (!nextBatch.isDone()) {
					break;
				}
				start(RediSearchSession.result(reader, cursor, join(nextBatch)));
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
		if (nextBatch != null) {
			// Deleted once the read in flight finishes, unless it exhausted the cursor. Waiting for it here would hold
			// up Trino's thread.
			Cursor current = cursor.orElseThrow();
			nextBatch.whenComplete((reply, failure) -> {
				if (reply == null || reply.getCursor().filter(next -> next.getCursorId() != 0).isPresent()) {
					session.cursorDeleteAsync(table, current);
				}
			});
			nextBatch = null;
		}
		cursor = Optional.empty();
	}
}
