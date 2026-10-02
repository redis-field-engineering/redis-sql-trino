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

import static com.google.common.base.Verify.verify;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.airlift.log.Logger;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

public class RediSearchPageSource implements ConnectorPageSource {

	private static final Logger log = Logger.get(RediSearchPageSource.class);

	private static final int ROWS_PER_REQUEST = 1024;

	private final RediSearchPageSourceResultWriter writer = new RediSearchPageSourceResultWriter();
	private final String[] columnNames;
	private final List<Type> columnTypes;
	private final CursorIterator iterator;
	private Map<String, String> currentDoc;
	private long completedBytes;
	private boolean finished;

	private final PageBuilder pageBuilder;

	public RediSearchPageSource(RediSearchSession session, RediSearchTableHandle table,
			List<RediSearchColumnHandle> columns) {
		this.columnNames = columns.stream().map(RediSearchColumnHandle::getName).toArray(String[]::new);
		this.iterator = new CursorIterator(session, table, columns);
		this.columnTypes = columns.stream().map(RediSearchColumnHandle::getType).collect(Collectors.toList());
		this.currentDoc = null;
		this.pageBuilder = new PageBuilder(columnTypes);
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
	public SourcePage getNextSourcePage() {
		verify(pageBuilder.isEmpty());
		for (int i = 0; i < ROWS_PER_REQUEST; i++) {
			if (!iterator.hasNext()) {
				finished = true;
				break;
			}
			currentDoc = iterator.next();

			pageBuilder.declarePosition();
			for (int column = 0; column < columnTypes.size(); column++) {
				BlockBuilder output = pageBuilder.getBlockBuilder(column);
				String value = currentValue(columnNames[column]);
				if (value == null) {
					output.appendNull();
				} else {
					writer.appendTo(columnTypes.get(column), value, output);
				}
			}
		}
		Page page = pageBuilder.build();
		pageBuilder.reset();
		// Trino requires completed bytes to be cumulative across pages
		completedBytes += page.getSizeInBytes();
		return SourcePage.create(page);
	}

	private String currentValue(String columnName) {
		if (RediSearchBuiltinField.isKeyColumn(columnName)) {
			return currentDoc.get(RediSearchBuiltinField.KEY.getName());
		}
		return currentDoc.get(columnName);
	}

	@Override
	public void close() {
		try {
			iterator.close();
		} catch (Exception e) {
			log.error(e, "Could not close cursor iterator");
		}
	}

	private static class CursorIterator implements Iterator<Map<String, String>>, AutoCloseable {

		private final RediSearchSession session;
		private final RediSearchTableHandle table;
		private Iterator<Map<String, String>> iterator;
		private Optional<Cursor> cursor;
		private RediSearchRowReader reader;

		public CursorIterator(RediSearchSession session, RediSearchTableHandle table,
				List<RediSearchColumnHandle> columns) {
			this.session = session;
			this.table = table;
			read(session.aggregate(table, columns));
		}

		private void read(RediSearchSession.AggregateResult results) {
			this.iterator = results.getRows().iterator();
			this.cursor = results.getCursor();
			this.reader = results.getReader();
		}

		@Override
		public boolean hasNext() {
			while (!iterator.hasNext()) {
				if (cursor.isEmpty()) {
					return false;
				}
				read(session.cursorRead(table, reader, cursor.get()));
			}
			return true;
		}

		@Override
		public Map<String, String> next() {
			return iterator.next();
		}

		@Override
		public void close() throws Exception {
			if (cursor.isPresent()) {
				session.cursorDelete(table, cursor.get());
				cursor = Optional.empty();
			}
		}

	}
}
