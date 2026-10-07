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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

public class RediSearchAggregation {

	public static final String MAX = "max";
	public static final String MIN = "min";
	public static final String AVG = "avg";
	public static final String SUM = "sum";
	public static final String COUNT = "count";
	private static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = Arrays.asList(MAX, MIN, AVG, SUM, COUNT);
	private static final List<Type> NUMERIC_TYPES = Arrays.asList(REAL, DOUBLE, TINYINT, SMALLINT, INTEGER, BIGINT);
	// Epoch milliseconds, which only have a smallest and largest: integers, which Redis formats in full
	private static final List<Type> TIMESTAMP_TYPES = Arrays.asList(TIMESTAMP_MILLIS, TIMESTAMP_TZ_MILLIS);
	private final String functionName;
	private final Type outputType;
	private final Optional<RediSearchColumnHandle> columnHandle;
	private final String alias;
	private final boolean countingValues;

	public RediSearchAggregation(String functionName, Type outputType, Optional<RediSearchColumnHandle> columnHandle,
			String alias) {
		this(functionName, outputType, columnHandle, alias, false);
	}

	/**
	 * @param countingValues whether a sum or average is computed from the sum and the number of the column's values,
	 *                       which {@link #valueExpression} and {@link #hasValueExpression} return for each document,
	 *                       rather than by Redis's SUM or AVG of the column
	 */
	@JsonCreator
	public RediSearchAggregation(@JsonProperty("functionName") String functionName,
			@JsonProperty("outputType") Type outputType,
			@JsonProperty("columnHandle") Optional<RediSearchColumnHandle> columnHandle,
			@JsonProperty("alias") String alias, @JsonProperty("countingValues") boolean countingValues) {
		this.functionName = functionName;
		this.outputType = outputType;
		this.columnHandle = columnHandle;
		this.alias = alias;
		this.countingValues = countingValues;
	}

	@JsonProperty
	public String getFunctionName() {
		return functionName;
	}

	@JsonProperty
	public Type getOutputType() {
		return outputType;
	}

	@JsonProperty
	public Optional<RediSearchColumnHandle> getColumnHandle() {
		return columnHandle;
	}

	@JsonProperty
	public String getAlias() {
		return alias;
	}

	/**
	 * Whether the aggregation is computed from {@link #getAlias()}, the sum of {@link #valueExpression}, and
	 * {@link #getCountAlias()}, the sum of {@link #hasValueExpression}. Redis's SUM and AVG would return nan for a group
	 * with no values on one of a sharded database's shards, which its coordinator adds to the other shards' sums, and
	 * the coordinator divides an average by the number of documents rather than of values.
	 */
	@JsonProperty
	public boolean isCountingValues() {
		return countingValues;
	}

	/**
	 * @return the number of the column's values in the group, for an aggregation {@link #isCountingValues counting
	 *         values}
	 */
	public String getCountAlias() {
		return "__count_" + alias;
	}

	/**
	 * @return the field the APPLY step of {@link #valueExpression} returns, which the sum reduces
	 */
	public String getValueField() {
		return "__value_" + alias;
	}

	/**
	 * @return the field the APPLY step of {@link #hasValueExpression} returns, which the count reduces
	 */
	public String getHasValueField() {
		return "__has_" + alias;
	}

	/**
	 * A conservative bound on every partial sum, including sums before negative values cancel.
	 */
	@JsonIgnore
	public OptionalLong getIntegerSumRowLimit() {
		if (!SUM.equals(functionName) || columnHandle.filter(RediSearchColumnHandle::isIntegerWidening).isEmpty()) {
			return OptionalLong.empty();
		}
		Type source = columnHandle.orElseThrow().getExpression().orElseThrow().getColumnType().orElseThrow();
		long magnitude = source.equals(TINYINT) ? 128 : source.equals(SMALLINT) ? 32768 : 2147483648L;
		return OptionalLong.of(((1L << 53) - 1) / magnitude);
	}

	public boolean isIntegerSumSafe(OptionalLong documents) {
		OptionalLong limit = getIntegerSumRowLimit();
		return limit.isEmpty() || (documents.isPresent() && documents.getAsLong() >= 0
				&& documents.getAsLong() <= limit.getAsLong());
	}

	public String integerSumCountAlias() {
		return "__precision_count_" + alias;
	}

	/**
	 * The column's value, or 0 for a document without one, so that every document adds a number to the sum. The
	 * operators and functions of other expressions fail on a missing value; case evaluates only the branch it takes.
	 */
	public String valueExpression() {
		RediSearchColumnHandle column = columnHandle.orElseThrow();
		String value = column.getExpression().map(RediSearchExpression::toRedis).orElse("@" + column.getName());
		return "case(" + hasValueExpression() + ", " + value + ", 0)";
	}

	/**
	 * 1 for a document with a value, and 0 for one without: for arithmetic, one with all of its columns' values.
	 */
	public String hasValueExpression() {
		return String.join(" && ", getInputFields().stream().map(field -> "exists(@" + field + ")").toList());
	}

	/**
	 * @return the fields the aggregation reads: its column's, or the columns its arithmetic reads
	 */
	public List<String> getInputFields() {
		RediSearchColumnHandle column = columnHandle.orElseThrow();
		return column.getExpression().map(expression -> List.copyOf(expression.getColumns().keySet()))
				.orElse(List.of(column.getName()));
	}

	/**
	 * Whether this reducer's value means its group had no values to aggregate, which SQL represents as null. Redis
	 * returns nan for SUM and AVG, and inf and -inf for MIN and MAX, which an index of infinite values would also give.
	 * An aggregation {@link #isCountingValues counting values} has none when its count is 0, and its sum is never
	 * empty.
	 */
	public boolean isEmptyResult(String value) {
		if (countingValues) {
			return false;
		}
		switch (functionName) {
		case SUM:
		case AVG:
			return "nan".equalsIgnoreCase(value);
		case MIN:
			return "inf".equalsIgnoreCase(value);
		case MAX:
			return "-inf".equalsIgnoreCase(value);
		default:
			return false;
		}
	}

	public static boolean isNumericType(Type type) {
		return NUMERIC_TYPES.contains(type);
	}

	/**
	 * @param countValues whether to compute sums and averages {@link #isCountingValues counting values}, which needs
	 *                    Redis's case function
	 */
	public static Optional<RediSearchAggregation> handleAggregation(AggregateFunction function,
			Map<String, ColumnHandle> assignments, String alias, boolean countValues) {
		if (!SUPPORTED_AGGREGATION_FUNCTIONS.contains(function.getFunctionName())) {
			return Optional.empty();
		}
		// Reducers aggregate every document in a group
		if (function.isDistinct() || function.getFilter().isPresent()) {
			return Optional.empty();
		}
		if (COUNT.equals(function.getFunctionName())) {
			// COUNT counts documents, which is count(*). count(column) skips nulls, so Trino computes it.
			if (!function.getArguments().isEmpty()) {
				return Optional.empty();
			}
			return Optional.of(new RediSearchAggregation(COUNT, function.getOutputType(), Optional.empty(), alias));
		}
		// Other variants, such as max(x, n), return arrays
		if (function.getArguments().size() != 1) {
			return Optional.empty();
		}
		Optional<RediSearchColumnHandle> parameterColumnHandle = function.getArguments().stream()
				.filter(Variable.class::isInstance).map(Variable.class::cast).map(Variable::getName)
				.filter(assignments::containsKey).findFirst().map(assignments::get)
				.map(RediSearchColumnHandle.class::cast)
				.filter(column -> isNumericType(column.getType())
						|| ((MIN.equals(function.getFunctionName()) || MAX.equals(function.getFunctionName()))
								&& TIMESTAMP_TYPES.contains(column.getType())));
		if (parameterColumnHandle.isEmpty()) {
			return Optional.empty();
		}
		String functionName = function.getFunctionName();
		RediSearchColumnHandle column = parameterColumnHandle.get();
		// Expressions can only refer to a field whose name is a property; arithmetic only refers to those
		boolean countingValues = countValues && (SUM.equals(functionName) || AVG.equals(functionName))
				&& (column.getExpression().isPresent() || RediSearchQueryBuilder.isProperty(column.getName()));
		// Only the sum and average of arithmetic, which an APPLY step computes, and only with case(), which keeps a
		// document without a value from failing it. Trino orders nan as larger than other doubles, where Redis's MIN
		// and MAX don't.
		if (column.getExpression().isPresent() && !countingValues) {
			return Optional.empty();
		}
		return Optional.of(new RediSearchAggregation(functionName, function.getOutputType(), parameterColumnHandle,
				alias, countingValues));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RediSearchAggregation that = (RediSearchAggregation) o;
		return Objects.equals(functionName, that.functionName) && Objects.equals(outputType, that.outputType)
				&& Objects.equals(columnHandle, that.columnHandle) && Objects.equals(alias, that.alias)
				&& countingValues == that.countingValues;
	}

	@Override
	public int hashCode() {
		return Objects.hash(functionName, outputType, columnHandle, alias, countingValues);
	}

	@Override
	public String toString() {
		return String.format("%s(%s)", functionName, columnHandle.map(column -> column.getExpression()
				.map(RediSearchExpression::toRedis).orElse(column.getName())).orElse(""));
	}
}
