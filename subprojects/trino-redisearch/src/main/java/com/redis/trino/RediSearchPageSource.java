package com.redis.trino;

import static com.google.common.base.Verify.verify;
import static com.redis.trino.TypeUtils.isJsonType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToIntBits;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.primitives.SignedBytes;
import com.redis.lettucemod.search.Document;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public class RediSearchPageSource implements ConnectorPageSource {
	private static final int ROWS_PER_REQUEST = 1024;

	private final Iterator<Document<String, String>> cursor;
	private final List<String> columnNames;
	private final List<Type> columnTypes;
	private Document<String, String> currentDoc;
	private long count;
	private boolean finished;

	private final PageBuilder pageBuilder;

	public RediSearchPageSource(RediSearchSession rediSearchSession, RediSearchTableHandle tableHandle,
			List<RediSearchColumnHandle> columns) {
		this.columnNames = columns.stream().map(RediSearchColumnHandle::getName).collect(toList());
		this.columnTypes = columns.stream().map(RediSearchColumnHandle::getType).collect(toList());
		this.cursor = rediSearchSession.execute(tableHandle, columns).iterator();
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
				appendTo(columnTypes.get(column), currentDoc.get(columnNames.get(column)), output);
			}
		}

		Page page = pageBuilder.build();
		pageBuilder.reset();
		return page;
	}

	private void appendTo(Type type, String value, BlockBuilder output) {
		if (value == null) {
			output.appendNull();
			return;
		}

		Class<?> javaType = type.getJavaType();
		if (javaType == boolean.class) {
			type.writeBoolean(output, Boolean.parseBoolean(value));
		} else if (javaType == long.class) {
			if (type.equals(BIGINT)) {
				type.writeLong(output, Long.parseLong(value));
			} else if (type.equals(INTEGER)) {
				type.writeLong(output, Integer.parseInt(value));
			} else if (type.equals(SMALLINT)) {
				type.writeLong(output, Short.parseShort(value));
			} else if (type.equals(TINYINT)) {
				type.writeLong(output, SignedBytes.checkedCast(Long.parseLong(value)));
			} else if (type.equals(REAL)) {
				type.writeLong(output, floatToIntBits((Float.parseFloat(value))));
			} else if (type instanceof DecimalType) {
				type.writeLong(output, encodeShortScaledValue(new BigDecimal(value), ((DecimalType) type).getScale()));
			} else if (type.equals(DATE)) {
				type.writeLong(output, LocalDate.from(DateTimeFormatter.ISO_DATE.parse(value)).toEpochDay());
			} else if (type.equals(TIMESTAMP_MILLIS)) {
				type.writeLong(output, Long.parseLong(value) * MICROSECONDS_PER_MILLISECOND);
			} else if (type.equals(TIMESTAMP_TZ_MILLIS)) {
				type.writeLong(output, packDateTimeWithZone(Long.parseLong(value), UTC_KEY));
			} else {
				throw new TrinoException(GENERIC_INTERNAL_ERROR,
						"Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
			}
		} else if (javaType == double.class) {
			type.writeDouble(output, Double.parseDouble(value));
		} else if (javaType == Slice.class) {
			writeSlice(output, type, value);
		} else {
			throw new TrinoException(GENERIC_INTERNAL_ERROR,
					"Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
		}
	}

	private void writeSlice(BlockBuilder output, Type type, String value) {
		if (type instanceof VarcharType) {
			type.writeSlice(output, utf8Slice(value));
		} else if (type instanceof CharType) {
			type.writeSlice(output, truncateToLengthAndTrimSpaces(utf8Slice(value), ((CharType) type)));
		} else if (type instanceof DecimalType) {
			type.writeObject(output, encodeScaledValue(new BigDecimal(value), ((DecimalType) type).getScale()));
		} else if (isJsonType(type)) {
			type.writeSlice(output, jsonParse(utf8Slice(value)));
		} else {
			throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
		}
	}

	public static JsonGenerator createJsonGenerator(JsonFactory factory, SliceOutput output) throws IOException {
		return factory.createGenerator((OutputStream) output);
	}

	@Override
	public void close() {

	}
}
