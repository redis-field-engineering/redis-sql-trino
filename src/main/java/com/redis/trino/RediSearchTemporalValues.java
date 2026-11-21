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

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

/** Lossless encodings for temporal types beyond the existing epoch-millisecond NUMERIC representation. */
final class RediSearchTemporalValues {
	private RediSearchTemporalValues() {}

	static boolean supports(Type type) {
		return type instanceof TimestampType && !type.equals(TimestampType.TIMESTAMP_MILLIS)
				|| type instanceof TimestampWithTimeZoneType && !type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)
				|| type instanceof TimeWithTimeZoneType;
	}

	static String encode(Type type, Block block, int position) {
		if (type.getJavaType() == long.class) {
			return Long.toString(type.getLong(block, position));
		}
		Object value = type.getObject(block, position);
		if (value instanceof LongTimestamp timestamp) {
			return timestamp.getEpochMicros() + ":" + timestamp.getPicosOfMicro();
		}
		if (value instanceof LongTimestampWithTimeZone timestamp) {
			return timestamp.getEpochMillis() + ":" + timestamp.getPicosOfMilli() + ":" + timestamp.getTimeZoneKey();
		}
		LongTimeWithTimeZone time = (LongTimeWithTimeZone) value;
		return time.getPicoseconds() + ":" + time.getOffsetMinutes();
	}

	static void decode(Type type, BlockBuilder output, String encoded) {
		try {
			String[] parts = encoded.split(":", -1);
			if (type instanceof TimestampType timestamp) {
				long micros = Long.parseLong(parts[0]);
				if (timestamp.isShort()) {
					check(parts.length == 1 && micros % power(6 - timestamp.getPrecision()) == 0);
					type.writeLong(output, micros);
				} else {
					check(parts.length == 2);
					int fraction = Integer.parseInt(parts[1]);
					check(fraction >= 0 && fraction < 1_000_000 && fraction % power(12 - timestamp.getPrecision()) == 0);
					type.writeObject(output, new LongTimestamp(micros, fraction));
				}
			} else if (type instanceof TimestampWithTimeZoneType timestamp) {
				long millis = Long.parseLong(parts[0]);
				if (timestamp.isShort()) {
					check(parts.length == 1);
					DateTimeEncoding.unpackZoneKey(millis);
					check(DateTimeEncoding.unpackMillisUtc(millis) % power(3 - timestamp.getPrecision()) == 0);
					type.writeLong(output, millis);
				} else {
					check(parts.length == 3);
					int fraction = Integer.parseInt(parts[1]);
					check(fraction >= 0 && fraction < 1_000_000_000 && fraction % power(12 - timestamp.getPrecision()) == 0);
					type.writeObject(output, LongTimestampWithTimeZone.fromEpochMillisAndFraction(millis, fraction,
							TimeZoneKey.getTimeZoneKey(Short.parseShort(parts[2]))));
				}
			} else {
				TimeWithTimeZoneType time = (TimeWithTimeZoneType) type;
				long value = Long.parseLong(parts[0]);
				long picos;
				int offset;
				if (time.isShort()) {
					check(parts.length == 1);
					picos = Math.multiplyExact(DateTimeEncoding.unpackTimeNanos(value), 1000);
					offset = DateTimeEncoding.unpackOffsetMinutes(value);
				} else {
					check(parts.length == 2);
					picos = value;
					offset = Integer.parseInt(parts[1]);
				}
				check(picos >= 0 && picos < 86_400_000_000_000_000L && picos % power(12 - time.getPrecision()) == 0
						&& offset >= -14 * 60 && offset <= 14 * 60);
				if (time.isShort()) {
					type.writeLong(output, value);
				} else {
					type.writeObject(output, new LongTimeWithTimeZone(picos, offset));
				}
			}
		} catch (IllegalArgumentException | IndexOutOfBoundsException | ArithmeticException e) {
			throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid encoded value for " + type, e);
		}
	}

	private static long power(int exponent) {
		return (long) Math.pow(10, exponent);
	}

	private static void check(boolean valid) {
		if (!valid) {
			throw new IllegalArgumentException("Invalid temporal encoding");
		}
	}
}
