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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.Document;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class RediSearchPageSource implements UpdatablePageSource {

	private static final int ROWS_PER_REQUEST = 1024;

	private final RediSearchPageSourceResultWriter writer = new RediSearchPageSourceResultWriter();
	private final RediSearchSession session;
	private final RediSearchTableHandle table;
	private final Iterator<Document<String, String>> cursor;
	private final String[] columnNames;
	private final List<Type> columnTypes;
	private final PageBuilder pageBuilder;

	private Document<String, String> currentDoc;
	private long count;
	private boolean finished;

	public RediSearchPageSource(RediSearchSession session, RediSearchTableHandle table,
			List<RediSearchColumnHandle> columns) {
		this.session = session;
		this.table = table;
		this.columnNames = columns.stream().map(RediSearchColumnHandle::getName).toArray(String[]::new);
		this.columnTypes = columns.stream().map(RediSearchColumnHandle::getType)
				.collect(Collectors.toList());
		this.cursor = session.search(table, columnNames).iterator();
		this.currentDoc = null;
		this.pageBuilder = new PageBuilder(columnTypes);
	}

	@Override
	public long getCompletedBytes() {
		return count;
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
	public long getMemoryUsage() {
		return 0L;
	}

	@Override
	public Page getNextPage() {
		verify(pageBuilder.isEmpty());
		count = 0;
		for (int i = 0; i < ROWS_PER_REQUEST; i++) {
			if (!cursor.hasNext()) {
				finished = true;
				break;
			}
			currentDoc = cursor.next();
			count++;

			pageBuilder.declarePosition();
			for (int column = 0; column < columnTypes.size(); column++) {
				BlockBuilder output = pageBuilder.getBlockBuilder(column);
				String columnName = columnNames[column];
				String value = currentValue(columnName);
				if (value == null) {
					output.appendNull();
				} else {
					writer.appendTo(columnTypes.get(column), value, output);
				}
			}
		}

		Page page = pageBuilder.build();
		pageBuilder.reset();
		return page;
	}

	@Override
	public void deleteRows(Block rowIds) {
		List<String> docIds = new ArrayList<>(rowIds.getPositionCount());
		for (int position = 0; position < rowIds.getPositionCount(); position++) {
			docIds.add(VarcharType.VARCHAR.getSlice(rowIds, position).toStringUtf8());
		}
		session.deleteDocs(docIds);
	}

	@Override
	public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels) {
		int rowIdChannel = columnValueAndRowIdChannels.get(columnValueAndRowIdChannels.size() - 1);
		List<Integer> columnChannelMapping = columnValueAndRowIdChannels.subList(0,
				columnValueAndRowIdChannels.size() - 1);
		StatefulRedisModulesConnection<String, String> connection = session.getConnection();
		connection.setAutoFlushCommands(false);
		RedisModulesAsyncCommands<String, String> commands = connection.async();
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (int position = 0; position < page.getPositionCount(); position++) {
			Block rowIdBlock = page.getBlock(rowIdChannel);
			if (rowIdBlock.isNull(position)) {
				continue;
			}
			String key = VarcharType.VARCHAR.getSlice(rowIdBlock, position).toStringUtf8();
			Map<String, String> map = new HashMap<>();
			for (int channel = 0; channel < columnChannelMapping.size(); channel++) {
				RediSearchColumnHandle column = table.getUpdatedColumns().get(columnChannelMapping.get(channel));
				Block block = page.getBlock(channel);
				if (block.isNull(position)) {
					continue;
				}
				String value = RediSearchPageSink.value(column.getType(), block, position);
				map.put(column.getName(), value);
			}
			RedisFuture<Long> future = commands.hset(key, map);
			futures.add(future);
		}
		connection.flushCommands();
		LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
		connection.setAutoFlushCommands(true);
	}

	private String currentValue(String columnName) {
		if (RediSearchBuiltinField.isBuiltinColumn(columnName)) {
			if (RediSearchBuiltinField.ID.getName().equals(columnName)) {
				return currentDoc.getId();
			}
			if (RediSearchBuiltinField.SCORE.getName().equals(columnName)) {
				return String.valueOf(currentDoc.getScore());
			}
		}
		return currentDoc.get(columnName);
	}

	@Override
	public CompletableFuture<Collection<Slice>> finish() {
		CompletableFuture<Collection<Slice>> future = new CompletableFuture<>();
		future.complete(Collections.emptyList());
		return future;
	}

	public static JsonGenerator createJsonGenerator(JsonFactory factory, SliceOutput output) throws IOException {
		return factory.createGenerator((OutputStream) output);
	}

	@Override
	public void close() {
		// nothing to do
	}
}
