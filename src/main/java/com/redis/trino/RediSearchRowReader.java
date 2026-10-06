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

import static io.trino.spi.type.RealType.REAL;
import static java.util.Locale.ENGLISH;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;

import io.lettuce.core.search.FieldValue;
import io.trino.spi.type.Type;

/**
 * Reads the values of an FT.AGGREGATE row in column order, from the fields the scan loaded them as. Other fields, such
 * as the rest of a hash loaded with LOAD *, are skipped.
 */
public class RediSearchRowReader {

	private static final JsonFactory JSON = new JsonFactory();

	private final List<String> columns;
	// The field each column is read from
	private final String[] fields;
	private final boolean[] jsonArrays;
	// The hash field to read each BIGINT column's value from again, if Redis may have rounded it
	private final String[] exactFields;
	private final int[] exactPositions;
	// The reducer whose result each column is, if any
	private final RediSearchAggregation[] metrics;
	// The type of each column whose value is also returned as a mantissa and exponent
	private final Type[] exactNumbers;
	// The columns the scan returns, if it computes arithmetic from the columns read
	private final Optional<List<RediSearchColumnHandle>> outputs;
	private final Map<String, Integer> positions;

	/**
	 * @param columns    the columns to read, in the order {@link #read} returns their values
	 * @param sources    the field each column is read from, for columns not read from the field of their name
	 * @param exactSources the hash field to read each BIGINT column loaded by name from again, if Redis may have
	 *                   rounded the value it returned
	 * @param jsonArrays columns whose values come as JSON arrays of the values at their JSON path, as DIALECT 3
	 *                   returns them
	 * @param metrics    the reducers whose results the aggregation returns, under their aliases
	 * @param exactNumbers the types of the columns whose values are also returned as the fields
	 *                   {@link RediSearchExactNumbers} names after their positions
	 */
	public RediSearchRowReader(List<String> columns, Map<String, String> sources, Map<String, String> exactSources,
			Set<String> jsonArrays, List<RediSearchAggregation> metrics, Map<String, Type> exactNumbers) {
		this(columns, sources, exactSources, jsonArrays, metrics, exactNumbers, Optional.empty());
	}

	/**
	 * @param outputs the columns the scan returns, which {@link #project} computes from the rows {@link #read}
	 *                returns, if it reads other columns than those to compute arithmetic from
	 */
	public RediSearchRowReader(List<String> columns, Map<String, String> sources, Map<String, String> exactSources,
			Set<String> jsonArrays, List<RediSearchAggregation> metrics, Map<String, Type> exactNumbers,
			Optional<List<RediSearchColumnHandle>> outputs) {
		this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
		requireNonNull(sources, "sources is null");
		requireNonNull(exactSources, "exactSources is null");
		requireNonNull(jsonArrays, "jsonArrays is null");
		requireNonNull(metrics, "metrics is null");
		requireNonNull(exactNumbers, "exactNumbers is null");
		this.fields = new String[this.columns.size()];
		this.jsonArrays = new boolean[this.columns.size()];
		this.metrics = new RediSearchAggregation[this.columns.size()];
		this.exactFields = new String[this.columns.size()];
		this.exactNumbers = new Type[this.columns.size()];
		for (int i = 0; i < fields.length; i++) {
			String column = this.columns.get(i);
			fields[i] = sources.getOrDefault(column, column);
			exactFields[i] = exactSources.get(column);
			this.jsonArrays[i] = jsonArrays.contains(column);
			this.exactNumbers[i] = exactNumbers.get(column);
			for (RediSearchAggregation metric : metrics) {
				if (metric.getAlias().equals(column)) {
					this.metrics[i] = metric;
				}
			}
		}
		this.exactPositions = IntStream.range(0, exactFields.length).filter(i -> exactFields[i] != null).toArray();
		this.outputs = requireNonNull(outputs, "outputs is null").map(List::copyOf);
		this.positions = IntStream.range(0, this.columns.size()).boxed()
				.collect(toImmutableMap(this.columns::get, identity()));
	}

	/**
	 * @param row the values {@link #read} returned, with the exact values of {@link #getExactPositions}
	 * @return the value of each column the scan returns: the row, or arithmetic computed from it
	 */
	public String[] project(String[] row) {
		if (outputs.isEmpty()) {
			return row;
		}
		List<RediSearchColumnHandle> columns = outputs.get();
		String[] values = new String[columns.size()];
		for (int i = 0; i < values.length; i++) {
			RediSearchColumnHandle column = columns.get(i);
			values[i] = column.getExpression().isPresent()
					? column.getExpression().get().evaluate(name -> row[positions.get(name)])
					: row[positions.get(column.getName())];
		}
		return values;
	}

	public List<String> getColumns() {
		return columns;
	}

	/**
	 * @return the positions of the columns whose values {@link #isPossiblyRounded} checks
	 */
	public int[] getExactPositions() {
		return exactPositions;
	}

	/**
	 * Whether Redis may have rounded a BIGINT value it returned: it formats an integer in full, so only one of 2^53 or
	 * more, or one that isn't an integer at all, may differ from the value stored.
	 */
	public static boolean isPossiblyRounded(String value) {
		try {
			return !RediSearchQueryBuilder.isExactAsDouble(Long.parseLong(value));
		} catch (NumberFormatException e) {
			return true;
		}
	}

	/**
	 * @return the hash field to read the value of the column at a position in {@link #getExactPositions} from
	 */
	public String getExactField(int position) {
		return exactFields[position];
	}

	/**
	 * @return the value of each column, null for none
	 */
	public String[] read(Map<String, FieldValue> row) {
		String[] values = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			FieldValue field = row.get(fields[i]);
			if (field == null || field.isNull()) {
				continue;
			}
			String value = field.asString();
			if (jsonArrays[i]) {
				value = firstValue(value).orElse(null);
			}
			if (metrics[i] != null && metrics[i].isEmptyResult(value)) {
				value = null;
			}
			Optional<Long> count = Optional.empty();
			if (metrics[i] != null && metrics[i].isCountingValues()) {
				// Values are counted as doubles
				count = Optional.ofNullable(asString(row.get(metrics[i].getCountAlias())))
						.map(number -> (long) Double.parseDouble(number));
				if (count.filter(number -> number > 0).isEmpty()) {
					value = null;
				}
			}
			if (value != null && exactNumbers[i] != null) {
				value = RediSearchExactNumbers.read(value, asString(row.get(RediSearchExactNumbers.mantissaField(i))),
						asString(row.get(RediSearchExactNumbers.exponentField(i))), exactNumbers[i]);
			}
			if (value != null && count.isPresent()) {
				value = countedValue(metrics[i], value, count.get());
			}
			values[i] = value;
		}
		return values;
	}

	/**
	 * A sum or average {@link RediSearchAggregation#isCountingValues counting values}, from the sum and the number of
	 * values, as Trino computes it.
	 */
	private static String countedValue(RediSearchAggregation metric, String sum, long count) {
		if (!RediSearchAggregation.AVG.equals(metric.getFunctionName())) {
			return javaNumber(sum);
		}
		double average = Double.parseDouble(javaNumber(sum)) / count;
		return metric.getOutputType() == REAL ? Float.toString((float) average) : Double.toString(average);
	}

	// Redis writes infinities and nan, from values that overflow, as Java doesn't parse them
	private static String javaNumber(String value) {
		switch (value.toLowerCase(ENGLISH)) {
		case "nan":
		case "-nan":
			return Double.toString(Double.NaN);
		case "inf":
		case "+inf":
			return Double.toString(Double.POSITIVE_INFINITY);
		case "-inf":
			return Double.toString(Double.NEGATIVE_INFINITY);
		default:
			return value;
		}
	}

	private static String asString(FieldValue field) {
		return field == null || field.isNull() ? null : field.asString();
	}

	/**
	 * The row a global aggregation over no documents returns: count is 0 and the other metrics are null.
	 */
	public String[] emptyAggregation() {
		String[] values = new String[fields.length];
		for (int i = 0; i < metrics.length; i++) {
			if (metrics[i] != null && RediSearchAggregation.COUNT.equals(metrics[i].getFunctionName())) {
				values[i] = "0";
			}
		}
		return values;
	}

	/**
	 * The first value in a JSON array, as DIALECT 2 returns it: a string as its value, a number as written, so as many
	 * digits as were stored, a boolean as {@code 1} or {@code 0}, and an object or array as its JSON text. Empty for
	 * {@code null} or an empty array.
	 */
	static Optional<String> firstValue(String array) {
		try (JsonParser parser = JSON.createParser(array)) {
			if (parser.nextToken() != JsonToken.START_ARRAY) {
				throw new IllegalArgumentException("Not a JSON array: " + array);
			}
			JsonToken token = parser.nextToken();
			switch (token) {
			case END_ARRAY:
			case VALUE_NULL:
				return Optional.empty();
			case VALUE_TRUE:
				return Optional.of("1");
			case VALUE_FALSE:
				return Optional.of("0");
			case START_OBJECT:
			case START_ARRAY:
				int start = Math.toIntExact(parser.currentTokenLocation().getCharOffset());
				parser.skipChildren();
				int end = Math.toIntExact(parser.currentLocation().getCharOffset());
				return Optional.of(array.substring(start, end));
			default:
				return Optional.of(parser.getText());
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Invalid JSON array: " + array, e);
		}
	}
}
