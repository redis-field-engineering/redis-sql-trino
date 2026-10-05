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

import static com.redis.trino.RediSearchErrorCode.REDISEARCH_UNEXPECTED_RESULT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;

import io.lettuce.core.search.arguments.AggregateArgs;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

/**
 * Reads the doubles Redis computes, GROUPBY keys and REDUCE results, exactly. Redis formats them rounded to 12
 * significant digits unless they're integers, which it formats in full. So APPLY steps also return each one's
 * absolute value as an integer mantissa and an exponent, from which {@link #read} rebuilds it.
 */
final class RediSearchExactNumbers {

	private RediSearchExactNumbers() {
	}

	/**
	 * Whether values of a type can have digits that Redis would round: the types that hold more than integers.
	 */
	static boolean isFloatingPoint(Type type) {
		return type == DOUBLE || type == REAL;
	}

	static String mantissaField(int position) {
		return "__mantissa_" + position;
	}

	static String exponentField(int position) {
		return "__exponent_" + position;
	}

	/**
	 * Adds the APPLY steps that return a property's value as {@link #mantissaField} and {@link #exponentField}: the
	 * exponent e = floor(log2(|x|)), and the mantissa |x| * 2^(53 - e), an integer below 2^55 even if log2 rounds e
	 * by one.
	 * <p>
	 * Operators fail the query on a missing value, such as the GROUPBY key of the documents without the field, but
	 * math functions return nan, so the value is only read through {@code abs}. 2^(53 - e) is multiplied in two
	 * halves, neither of which overflows for any double, so the mantissa is exact.
	 */
	static void apply(AggregateArgs.Builder args, String property, int position) {
		String exponent = exponentField(position);
		args.apply(format("floor(log2(abs(@%s)))", property), exponent);
		args.apply(format("abs(@%1$s) * 2 ^ (26 - floor(@%2$s / 2)) * 2 ^ (27 - ceil(@%2$s / 2))", property, exponent),
				mantissaField(position));
	}

	/**
	 * @param value    a value Redis formatted
	 * @param mantissa the mantissa {@link #apply} returned for it, if any
	 * @param exponent the exponent {@link #apply} returned for it, if any
	 * @param type     the type the value is read as
	 * @return the value, with the digits Redis rounded
	 */
	static String read(String value, String mantissa, String exponent, Type type) {
		double exact;
		try {
			exact = Math.scalb((double) Long.parseLong(mantissa), Math.toIntExact(Long.parseLong(exponent)) - 53);
		} catch (NumberFormatException | ArithmeticException e) {
			// nan for zero, infinities and nan, which Redis formats in full
			return value;
		}
		if (value.startsWith("-")) {
			exact = -exact;
		}
		// Redis rounded the value it formatted to 12 significant digits
		double formatted = Double.parseDouble(value);
		if (Math.abs(exact - formatted) > Math.abs(formatted) * 1e-11) {
			throw new TrinoException(REDISEARCH_UNEXPECTED_RESULT,
					format("Redis returned mantissa %s and exponent %s for the value %s", mantissa, exponent, value));
		}
		return type == REAL ? Float.toString((float) exact) : Double.toString(exact);
	}
}
