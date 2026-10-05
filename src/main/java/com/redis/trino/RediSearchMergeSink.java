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

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;


import io.airlift.slice.Slice;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.SchemaTableName;

/**
 * Applies SQL MERGE, UPDATE and DELETE to Redis hashes. The row ID is the document key.
 * <p>
 * Page layout: data columns, then operation (TINYINT), update case number (INTEGER), row ID (VARCHAR).
 */
public class RediSearchMergeSink implements ConnectorMergeSink {

	private final RediSearchSession session;
	private final RediSearchPageSink insertSink;
	private final SchemaTableName schemaTableName;
	private final List<RediSearchColumnHandle> columns;
	private final Map<Integer, List<Integer>> updateCaseChannels;
	// Opened by the first page
	private RediSearchSession.Writer writer;

	public RediSearchMergeSink(RediSearchSession session, RediSearchPageSink insertSink,
			RediSearchMergeTableHandle handle) {
		this.session = requireNonNull(session, "session is null");
		this.insertSink = requireNonNull(insertSink, "insertSink is null");
		this.schemaTableName = handle.getTableHandle().getSchemaTableName();
		this.columns = handle.getDataColumns();
		this.updateCaseChannels = handle.getUpdateCaseChannels();
	}

	@Override
	public void storeMergedRows(Page page) {
		int columnCount = columns.size();
		Block operationBlock = page.getBlock(columnCount);
		Block caseNumberBlock = page.getBlock(columnCount + 1);
		Block rowIdBlock = page.getBlock(columnCount + 2);

		int[] insertPositions = new int[page.getPositionCount()];
		int insertCount = 0;
		List<String> deleteKeys = new ArrayList<>();
		if (writer == null) {
			writer = session.openWriter();
		}
		RedisClusterAsyncCommands<String, String> commands = writer.commands();
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (int position = 0; position < page.getPositionCount(); position++) {
			switch (TINYINT.getByte(operationBlock, position)) {
			case INSERT_OPERATION_NUMBER:
				insertPositions[insertCount++] = position;
				break;
			case DELETE_OPERATION_NUMBER:
				deleteKeys.add(key(rowIdBlock, position));
				break;
			case UPDATE_OPERATION_NUMBER:
				String key = key(rowIdBlock, position);
				List<Integer> channels = updateCaseChannels.get(INTEGER.getInt(caseNumberBlock, position));
				Map<String, String> values = new HashMap<>();
				List<String> nulls = new ArrayList<>();
				for (int channel : channels) {
					RediSearchColumnHandle column = columns.get(channel);
					Block block = page.getBlock(channel);
					if (block.isNull(position)) {
						nulls.add(column.getName());
					} else {
						values.put(column.getName(), RediSearchPageSink.value(column.getType(), block, position));
					}
				}
				if (!values.isEmpty()) {
					futures.add(commands.hset(key, values));
				}
				if (!nulls.isEmpty()) {
					futures.add(commands.hdel(key, nulls.toArray(String[]::new)));
				}
				break;
			default:
				throw new IllegalStateException("Unexpected merge operation");
			}
		}
		if (!deleteKeys.isEmpty()) {
			futures.add(commands.del(deleteKeys.toArray(String[]::new)));
		}
		if (insertCount > 0) {
			// Before changing anything: MERGE can insert into a JSON index, where the rows would be lost. The commands
			// so far are still in the writer's buffer.
			session.verifyWritable(schemaTableName);
		}
		writer.flush(futures);
		if (insertCount > 0) {
			Page dataPage = page.getColumns(IntStream.range(0, columnCount).toArray());
			insertSink.appendPage(dataPage.getPositions(insertPositions, 0, insertCount));
		}
	}

	private static String key(Block rowIdBlock, int position) {
		return VARCHAR.getSlice(rowIdBlock, position).toStringUtf8();
	}

	@Override
	public CompletableFuture<Collection<Slice>> finish() {
		closeWriter();
		return insertSink.finish();
	}

	@Override
	public void abort() {
		closeWriter();
		insertSink.abort();
	}

	private void closeWriter() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}
}
