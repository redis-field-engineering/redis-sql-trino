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

public class RediSearchPageSourceResultWriter {

	public void appendTo(Type type, String value, BlockBuilder output) {
		Class<?> javaType = type.getJavaType();
		if (javaType == boolean.class) {
			type.writeBoolean(output, Boolean.parseBoolean(value));
		} else if (javaType == long.class) {
			type.writeLong(output, getLong(type, value));
		} else if (javaType == double.class) {
			type.writeDouble(output, Double.parseDouble(value));
		} else if (javaType == Slice.class) {
			writeSlice(output, type, value);
		} else if (javaType == Int128.class) {
			// Long decimals
			type.writeObject(output, encodeScaledValue(new BigDecimal(value), ((DecimalType) type).getScale()));
		} else {
			throw new TrinoException(GENERIC_INTERNAL_ERROR,
					"Unhandled type for " + javaType.getSimpleName() + ":" + type.getDisplayName());
		}
	}

	private long getLong(Type type, String value) {
		if (type.equals(BIGINT)) {
			return parseInteger(type, value);
		}
		if (type.equals(INTEGER)) {
			return checkRange(type, value, Integer.MIN_VALUE, Integer.MAX_VALUE);
		}
		if (type.equals(SMALLINT)) {
			return checkRange(type, value, Short.MIN_VALUE, Short.MAX_VALUE);
		}
		if (type.equals(TINYINT)) {
			return checkRange(type, value, Byte.MIN_VALUE, Byte.MAX_VALUE);
		}
		if (type.equals(REAL)) {
			return floatToIntBits((Float.parseFloat(value)));
		}
		if (type instanceof DecimalType) {
			return encodeShortScaledValue(new BigDecimal(value), ((DecimalType) type).getScale());
		}
		if (type.equals(DATE)) {
			return LocalDate.from(DateTimeFormatter.ISO_DATE.parse(value)).toEpochDay();
		}
		if (type.equals(TIMESTAMP_MILLIS)) {
			return multiplyExact(parseInteger(type, value), MICROSECONDS_PER_MILLISECOND);
		}
		if (type.equals(TIMESTAMP_TZ_MILLIS)) {
			return packDateTimeWithZone(parseInteger(type, value), UTC_KEY);
		}
		throw new TrinoException(GENERIC_INTERNAL_ERROR,
				"Unhandled type for " + type.getJavaType().getSimpleName() + ":" + type.getDisplayName());
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

	private void writeSlice(BlockBuilder output, Type type, String value) {
		if (type instanceof VarcharType) {
			type.writeSlice(output, utf8Slice(value));
		} else if (type instanceof CharType) {
			type.writeSlice(output, truncateToLengthAndTrimSpaces(utf8Slice(value), (CharType) type));
		} else if (type.equals(UUID)) {
			type.writeSlice(output, javaUuidToTrinoUuid(java.util.UUID.fromString(value)));
		} else if (type.getBaseName().equals(JSON)) {
			type.writeSlice(output, io.trino.plugin.base.util.JsonTypeUtil.jsonParse(utf8Slice(value)));
		} else {
			throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getDisplayName());
		}
	}

}
