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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import com.github.f4b6a3.ulid.UlidFactory;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;

import io.airlift.slice.Slice;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

public class RediSearchPageSink implements ConnectorPageSink {

	private static final String KEY_SEPARATOR = ":";
	private final RediSearchSession session;
	private final SchemaTableName schemaTableName;
	private final List<RediSearchColumnHandle> columns;
	private final UlidFactory factory = UlidFactory.newInstance(new Random());
	private final RediSearchFlexWriteBuffer flexBuffer;
	// Opened by the first page
	private RediSearchSession.Writer writer;

	public RediSearchPageSink(RediSearchSession rediSearchSession, SchemaTableName schemaTableName,
			List<RediSearchColumnHandle> columns) {
		this.session = rediSearchSession;
		this.schemaTableName = schemaTableName;
		this.columns = columns;
		this.flexBuffer = session.getConfig().isFlexEnabled() ? new RediSearchFlexWriteBuffer(session) : null;
	}

	@Override
	public CompletableFuture<?> appendPage(Page page) {
		String prefix = prefix().orElse(schemaTableName.getTableName() + KEY_SEPARATOR);
		session.verifyWritable(schemaTableName);
		RediSearchIndexInfo info = session.getTable(schemaTableName).getIndexInfo();
		Map<String, String> fieldNames = RediSearchHashUpdate.fieldNames(info);
		RediSearchJsonWriter json = info.getKeyType().filter(RediSearchIndexInfo.KeyType.JSON::equals)
				.map(ignored -> new RediSearchJsonWriter(info)).orElse(null);
		// Convert the full page before buffering any mutation so unsupported JSON values fail before writes.
		List<Map<String, String>> rows = new ArrayList<>();
		List<String> documents = new ArrayList<>();
		for (int position = 0; position < page.getPositionCount(); position++) {
			Map<String, String> row = new HashMap<>();
			for (int channel = 0; channel < page.getChannelCount(); channel++) {
				if (!page.getBlock(channel).isNull(position)) {
					RediSearchColumnHandle column = columns.get(channel);
					String field = json != null ? column.getName() : flexBuffer != null
							? session.flexWriteField(schemaTableName, column.getName())
							: fieldNames.getOrDefault(column.getName(), column.getName());
					RediSearchHashUpdate.putValue(row, field, value(column.getType(), page.getBlock(channel), position));
				}
			}
			if (json == null) {
				row.put(RediSearchSession.ROW_MARKER, "1");
			}
			rows.add(row);
			if (json != null) {
				documents.add(json.document(row));
			}
		}
		if (writer == null) {
			writer = session.openWriter();
		}
		RedisClusterAsyncCommands<String, String> commands = writer.commands();
		List<RedisFuture<?>> futures = new ArrayList<>();
		List<String> staged = new ArrayList<>();
		for (int position = 0; position < page.getPositionCount(); position++) {
			String key = prefix + factory.create().toString();
			if (flexBuffer != null) {
				// Publish only after source scans finish; LIST entries cannot feed a Flex keyset scan.
				List<String> row = new ArrayList<>();
				row.add(key);
				rows.get(position).forEach((field, value) -> { row.add(field); row.add(value); });
				staged.add(RediSearchFlexWriteBuffer.ROW_CODEC.toJson(row));
			} else if (json != null) {
				futures.add(commands.jsonSet(key, JsonPath.ROOT_PATH, documents.get(position)));
			} else {
				futures.add(commands.hset(key, rows.get(position)));
			}
		}
		// Flush ordinary document writes, or persist only invisible staging records on Flex.
		if (flexBuffer == null) { writer.flush(futures); }
		else { flexBuffer.append(writer, staged); }
		return NOT_BLOCKED;
	}

	private Optional<String> prefix() {
		try {
			RediSearchTable table = session.getTable(schemaTableName);
			RediSearchIndexInfo indexInfo = table.getIndexInfo();
			if (indexInfo.getPrefixes().isEmpty()) {
				return Optional.empty();
			}
			String prefix = indexInfo.getPrefixes().get(0);
			if (session.getConfig().isFlexEnabled()) { RediSearchCursorFreePageSource.verifyKey(prefix); }
			if (prefix.equals("*") && !session.getConfig().isFlexEnabled()) {
				return Optional.empty();
			}
			if (prefix.endsWith(KEY_SEPARATOR)) {
				return Optional.of(prefix);
			}
			return Optional.of(prefix + KEY_SEPARATOR);
		} catch (RuntimeException e) {
			if (session.getConfig().isFlexEnabled()) { throw e; }
			return Optional.empty();
		}
	}

	public static String value(Type type, Block block, int position) {
		if (RediSearchComplexValues.isComplex(type)) {
			return RediSearchComplexValues.encode(type, block, position);
		}
		if (RediSearchTemporalValues.supports(type)) {
			return RediSearchTemporalValues.encode(type, block, position);
		}
		if (type.getBaseName().equals(io.trino.spi.type.StandardTypes.JSON)) {
			return type.getSlice(block, position).toStringUtf8();
		}
		if (type.equals(BooleanType.BOOLEAN)) {
			return String.valueOf(type.getBoolean(block, position));
		}
		if (type.equals(BigintType.BIGINT)) {
			return String.valueOf(type.getLong(block, position));
		}
		if (type.equals(IntegerType.INTEGER)) {
			return String.valueOf(toIntExact(type.getLong(block, position)));
		}
		if (type.equals(SmallintType.SMALLINT)) {
			return String.valueOf(Shorts.checkedCast(type.getLong(block, position)));
		}
		if (type.equals(TinyintType.TINYINT)) {
			return String.valueOf(SignedBytes.checkedCast(type.getLong(block, position)));
		}
		if (type.equals(RealType.REAL)) {
			return String.valueOf(intBitsToFloat(toIntExact(type.getLong(block, position))));
		}
		if (type.equals(DoubleType.DOUBLE)) {
			return String.valueOf(type.getDouble(block, position));
		}
		if (type instanceof VarcharType) {
			return type.getSlice(block, position).toStringUtf8();
		}
		if (type instanceof CharType) {
			return padSpaces(type.getSlice(block, position), (CharType) type).toStringUtf8();
		}
		if (type.equals(VarbinaryType.VARBINARY)) {
			return Base64.getEncoder().encodeToString(type.getSlice(block, position).getBytes());
		}
		if (type.equals(DateType.DATE)) {
			long days = type.getLong(block, position);
			return DateTimeFormatter.ISO_DATE.format(LocalDate.ofEpochDay(days));
		}
		if (type instanceof TimeType) {
			return String.valueOf(type.getLong(block, position));
		}
		if (type.equals(TIMESTAMP_MILLIS)) {
			long millisUtc = floorDiv(type.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
			return String.valueOf(millisUtc);
		}
		if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
			long millisUtc = unpackMillisUtc(type.getLong(block, position));
			return String.valueOf(millisUtc);
		}
		if (type instanceof DecimalType) {
			return readBigDecimal((DecimalType) type, block, position).toPlainString();
		}
		if (type.equals(UuidType.UUID)) {
			return trinoUuidToJavaUuid(type.getSlice(block, position)).toString();
		}
		throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
	}

	@Override
	public CompletableFuture<Collection<Slice>> finish() {
		closeWriter();
		return completedFuture(flexBuffer == null ? List.of() : flexBuffer.finish());
	}

	@Override
	public void abort() {
		closeWriter();
		if (flexBuffer != null) { flexBuffer.abort(); }
	}

	private void closeWriter() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}
}
