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
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.SchemaTableName;

/**
 * Applies SQL MERGE, UPDATE and DELETE to Redis HASH/JSON documents. The row ID is the document key.
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
		RediSearchIndexInfo info = session.getTable(schemaTableName).getIndexInfo();
		Map<String, String> fieldNames = RediSearchHashUpdate.fieldNames(info);
		boolean jsonIndex = info.getKeyType().filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent();
		RediSearchJsonWriter json = jsonIndex && !updateCaseChannels.isEmpty() ? new RediSearchJsonWriter(info) : null;
		int columnCount = columns.size();
		Block operationBlock = page.getBlock(columnCount);
		Block caseNumberBlock = page.getBlock(columnCount + 1);
		Block rowIdBlock = page.getBlock(columnCount + 2);
		for (int position = 0; position < page.getPositionCount(); position++) {
			byte operation = TINYINT.getByte(operationBlock, position);
			if (operation == INSERT_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
				session.verifyWritable(schemaTableName);
				break;
			}
		}
		Map<Integer, String> jsonUpdates = new HashMap<>();
		Map<Integer, String[]> atomicUpdates = new HashMap<>();
		boolean atomicHash = !jsonIndex && session.getConfig().isAtomicHashUpdatesEnabled();
		if (jsonIndex || atomicHash) {
			// Validate every JSON/atomic HASH mutation in the page, including insert-only MERGE branches, before flushing.
			for (int position = 0; position < page.getPositionCount(); position++) {
				byte operation = TINYINT.getByte(operationBlock, position);
				if (operation != INSERT_OPERATION_NUMBER && operation != UPDATE_OPERATION_NUMBER) {
					continue;
				}
				if (jsonIndex && json == null) {
					json = new RediSearchJsonWriter(info);
				}
				List<Integer> channels = operation == INSERT_OPERATION_NUMBER
						? IntStream.range(0, columnCount).boxed().toList()
						: updateCaseChannels.get(INTEGER.getInt(caseNumberBlock, position));
				Map<String, String> values = new HashMap<>();
				for (int channel : channels) {
					RediSearchColumnHandle column = columns.get(channel);
					Block block = page.getBlock(channel);
					String field = jsonIndex ? column.getName() : session.getConfig().isFlexEnabled()
							? session.flexWriteField(schemaTableName, column.getName())
							: fieldNames.getOrDefault(column.getName(), column.getName());
					if (!block.isNull(position)) {
						RediSearchHashUpdate.putValue(values, field, RediSearchPageSink.value(column.getType(), block, position));
					} else if (operation == UPDATE_OPERATION_NUMBER) {
						RediSearchHashUpdate.putValue(values, field, null);
					}
				}
				if (jsonIndex) {
					jsonUpdates.put(position, json.document(values));
				} else if (operation == UPDATE_OPERATION_NUMBER) {
					key(rowIdBlock, position);
					List<String> nulls = values.entrySet().stream().filter(entry -> entry.getValue() == null)
							.map(Map.Entry::getKey).toList();
					nulls.forEach(values::remove);
					values.put(RediSearchSession.ROW_MARKER, "1");
					atomicUpdates.put(position, RediSearchHashUpdate.arguments(values, nulls));
				}
			}
		}

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
				for (int channel : channels) {
					RediSearchColumnHandle column = columns.get(channel);
					Block block = page.getBlock(channel);
					String field = jsonIndex ? column.getName() : session.getConfig().isFlexEnabled()
							? session.flexWriteField(schemaTableName, column.getName())
							: fieldNames.getOrDefault(column.getName(), column.getName());
					RediSearchHashUpdate.putValue(values, field,
							block.isNull(position) ? null : RediSearchPageSink.value(column.getType(), block, position));
				}
				List<String> nulls = values.entrySet().stream().filter(entry -> entry.getValue() == null)
						.map(Map.Entry::getKey).toList();
				nulls.forEach(values::remove);
				if (json != null) {
					nulls.forEach(column -> values.put(column, null));
					// RFC 7396 patch: update only these paths, create missing parents, remove SQL NULL fields.
					futures.add(commands.jsonMerge(key, JsonPath.ROOT_PATH, jsonUpdates.get(position)));
				} else if (atomicHash) {
					futures.add(commands.<Long>eval(RediSearchHashUpdate.SCRIPT, ScriptOutputType.INTEGER,
							new String[] {key}, atomicUpdates.get(position)));
				} else {
					// A Redis hash disappears when its last field is removed. Keep the SQL row alive.
					values.put(RediSearchSession.ROW_MARKER, "1");
					if (!values.isEmpty()) {
						futures.add(commands.hset(key, values));
					}
					if (!nulls.isEmpty()) {
						futures.add(commands.hdel(key, nulls.toArray(String[]::new)));
					}
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
			// Validate mappings before sending the buffered mutations.
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
