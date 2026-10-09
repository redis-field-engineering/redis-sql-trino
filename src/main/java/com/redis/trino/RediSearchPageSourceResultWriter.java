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

import static io.airlift.slice.Slices.utf8Slice;
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
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

/**
 * Writes the values Redis returns, as strings, to Trino blocks.
 */
public final class RediSearchPageSourceResultWriter {

	/**
	 * Writes a non-null value of one column's type.
	 */
	@FunctionalInterface
	public interface ValueWriter {
		void write(BlockBuilder output, String value);
	}

	private RediSearchPageSourceResultWriter() {
	}

	/**
	 * The writer for a column's values, chosen once per column rather than for each value.
	 */
	public static ValueWriter writer(Type type) {
		Class<?> javaType = type.getJavaType();
		if (javaType == boolean.class) {
			return (output, value) -> type.writeBoolean(output, Boolean.parseBoolean(value));
		}
		if (javaType == long.class) {
			LongParser parser = longParser(type);
			return (output, value) -> type.writeLong(output, parser.parse(value));
		}
		if (javaType == double.class) {
			return (output, value) -> type.writeDouble(output, Double.parseDouble(value));
		}
		if (javaType == Slice.class) {
			return sliceWriter(type);
		}
		if (javaType == Int128.class) {
			// Long decimals
			int scale = ((DecimalType) type).getScale();
			return (output, value) -> type.writeObject(output, encodeScaledValue(new BigDecimal(value), scale));
		}
		return unhandled("Unhandled type for " + javaType.getSimpleName() + ":" + type.getDisplayName());
	}

	@FunctionalInterface
	private interface LongParser {
		long parse(String value);
	}

	private static LongParser longParser(Type type) {
		if (type.equals(BIGINT)) {
			return value -> parseInteger(type, value);
		}
		if (type.equals(INTEGER)) {
			return value -> checkRange(type, value, Integer.MIN_VALUE, Integer.MAX_VALUE);
		}
		if (type.equals(SMALLINT)) {
			return value -> checkRange(type, value, Short.MIN_VALUE, Short.MAX_VALUE);
		}
		if (type.equals(TINYINT)) {
			return value -> checkRange(type, value, Byte.MIN_VALUE, Byte.MAX_VALUE);
		}
		if (type.equals(REAL)) {
			return value -> floatToIntBits(Float.parseFloat(value));
		}
		if (type instanceof DecimalType decimalType) {
			int scale = decimalType.getScale();
			return value -> encodeShortScaledValue(new BigDecimal(value), scale);
		}
		if (type.equals(DATE)) {
			return value -> LocalDate.from(DateTimeFormatter.ISO_DATE.parse(value)).toEpochDay();
		}
		if (type.equals(TIMESTAMP_MILLIS)) {
			return RediSearchPageSourceResultWriter::timestampMicros;
		}
		if (type.equals(TIMESTAMP_TZ_MILLIS)) {
			return value -> packDateTimeWithZone(parseInteger(type, value), UTC_KEY);
		}
		String message = "Unhandled type for " + type.getJavaType().getSimpleName() + ":" + type.getDisplayName();
		return value -> {
			throw new TrinoException(GENERIC_INTERNAL_ERROR, message);
		};
	}

	static long timestampMicros(String value) {
		return multiplyExact(parseInteger(TIMESTAMP_MILLIS, value), MICROSECONDS_PER_MILLISECOND);
	}

	// Redis returns the results of reducers, and other clients may write integers, in other forms, e.g. 42.0 or 4.2e1
	private static long parseInteger(Type type, String value) {
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException e) {
			try {
				return new BigDecimal(value).longValueExact();
			} catch (ArithmeticException | NumberFormatException notAnInteger) {
				throw invalidValue(type, value);
			}
		}
	}

	private static long checkRange(Type type, String value, long min, long max) {
		long result = parseInteger(type, value);
		if (result < min || result > max) {
			throw invalidValue(type, value);
		}
		return result;
	}

	private static TrinoException invalidValue(Type type, String value) {
		return new TrinoException(GENERIC_INTERNAL_ERROR, format("Value '%s' is not a valid %s", value,
				type.getDisplayName()));
	}

	private static ValueWriter sliceWriter(Type type) {
		if (type instanceof VarcharType) {
			return (output, value) -> type.writeSlice(output, utf8Slice(value));
		}
		if (type instanceof CharType charType) {
			return (output, value) -> type.writeSlice(output, truncateToLengthAndTrimSpaces(utf8Slice(value), charType));
		}
		if (type.equals(UUID)) {
			return (output, value) -> type.writeSlice(output, javaUuidToTrinoUuid(java.util.UUID.fromString(value)));
		}
		if (type.getBaseName().equals(JSON)) {
			return (output, value) -> type.writeSlice(output,
					io.trino.plugin.base.util.JsonTypeUtil.jsonParse(utf8Slice(value)));
		}
		return unhandled("Unhandled type for Slice: " + type.getDisplayName());
	}

	// Fails only once there's a value to write, so a column of the type that's always null still reads
	private static ValueWriter unhandled(String message) {
		return (output, value) -> {
			throw new TrinoException(GENERIC_INTERNAL_ERROR, message);
		};
	}

}
