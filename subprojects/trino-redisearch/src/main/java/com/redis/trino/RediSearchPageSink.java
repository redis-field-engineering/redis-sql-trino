package com.redis.trino;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import com.github.f4b6a3.ulid.UlidFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.airlift.slice.Slice;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
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
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

public class RediSearchPageSink implements ConnectorPageSink {

	private final RediSearchSession rediSearchSession;
	private final SchemaTableName schemaTableName;
	private final List<RediSearchColumnHandle> columns;
	private final UlidFactory factory = UlidFactory.newInstance(new Random());

	public RediSearchPageSink(RediSearchClientConfig config, RediSearchSession rediSearchSession,
			SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns) {
		this.rediSearchSession = rediSearchSession;
		this.schemaTableName = schemaTableName;
		this.columns = columns;
	}

	@Override
	public CompletableFuture<?> appendPage(Page page) {
		StatefulRedisModulesConnection<String, String> connection = rediSearchSession.getConnection();
		RedisModulesAsyncCommands<String, String> async = connection.async();
		async.setAutoFlushCommands(false);
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (int position = 0; position < page.getPositionCount(); position++) {
			Map<String, String> map = new HashMap<>();
			String key = schemaTableName.getTableName() + ":" + factory.create().toString();
			for (int channel = 0; channel < page.getChannelCount(); channel++) {
				RediSearchColumnHandle column = columns.get(channel);
				Block block = page.getBlock(channel);
				if (block.isNull(position)) {
					continue;
				}
				map.put(column.getName(), getObjectValue(columns.get(channel).getType(), block, position));
			}
			RedisFuture<Long> future = async.hset(key, map);
			futures.add(future);
		}
		async.flushCommands();
		LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
		async.setAutoFlushCommands(true);
		return NOT_BLOCKED;
	}

	private String getObjectValue(Type type, Block block, int position) {
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
			return padSpaces(type.getSlice(block, position), ((CharType) type)).toStringUtf8();
		}
		if (type.equals(VarbinaryType.VARBINARY)) {
			return new String(type.getSlice(block, position).getBytes());
		}
		if (type.equals(DateType.DATE)) {
			long days = type.getLong(block, position);
			return DateTimeFormatter.ISO_DATE.format(LocalDate.ofEpochDay(days));
		}
		if (type.equals(TimeType.TIME_MILLIS)) {
			long picos = type.getLong(block, position);
			return String.valueOf(roundDiv(picos, PICOSECONDS_PER_MILLISECOND));
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
		throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
	}

	@Override
	public CompletableFuture<Collection<Slice>> finish() {
		return completedFuture(ImmutableList.of());
	}

	@Override
	public void abort() {
	}
}
