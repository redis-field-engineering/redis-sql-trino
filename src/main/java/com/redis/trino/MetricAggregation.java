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
import static io.trino.spi.type.TinyintType.TINYINT;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

public class MetricAggregation {

	public static final String MAX = "max";
	public static final String MIN = "min";
	public static final String AVG = "avg";
	public static final String SUM = "sum";
	public static final String COUNT = "count";
	private static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = Arrays.asList(MAX, MIN, AVG, SUM, COUNT);
	private static final List<Type> NUMERIC_TYPES = Arrays.asList(REAL, DOUBLE, TINYINT, SMALLINT, INTEGER, BIGINT);
	private final String functionName;
	private final Type outputType;
	private final Optional<RediSearchColumnHandle> columnHandle;
	private final String alias;

	@JsonCreator
	public MetricAggregation(@JsonProperty("functionName") String functionName,
			@JsonProperty("outputType") Type outputType,
			@JsonProperty("columnHandle") Optional<RediSearchColumnHandle> columnHandle,
			@JsonProperty("alias") String alias) {
		this.functionName = functionName;
		this.outputType = outputType;
		this.columnHandle = columnHandle;
		this.alias = alias;
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

	public static boolean isNumericType(Type type) {
		return NUMERIC_TYPES.contains(type);
	}

	public static Optional<MetricAggregation> handleAggregation(AggregateFunction function,
			Map<String, ColumnHandle> assignments, String alias) {
		if (!SUPPORTED_AGGREGATION_FUNCTIONS.contains(function.getFunctionName())) {
			return Optional.empty();
		}
		// check
		// 1. Function input can be found in assignments
		// 2. Target type of column being aggregate must be numeric type
		// 3. ColumnHandle support predicates(since text treats as VARCHAR, but text can
		// not be treats as term in es by default
		Optional<RediSearchColumnHandle> parameterColumnHandle = function.getArguments().stream()
				.filter(Variable.class::isInstance).map(Variable.class::cast).map(Variable::getName)
				.filter(assignments::containsKey).findFirst().map(assignments::get)
				.map(RediSearchColumnHandle.class::cast)
				.filter(column -> MetricAggregation.isNumericType(column.getType()));
		// only count can accept empty ElasticsearchColumnHandle
		if (!COUNT.equals(function.getFunctionName()) && parameterColumnHandle.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new MetricAggregation(function.getFunctionName(), function.getOutputType(),
				parameterColumnHandle, alias));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MetricAggregation that = (MetricAggregation) o;
		return Objects.equals(functionName, that.functionName) && Objects.equals(outputType, that.outputType)
				&& Objects.equals(columnHandle, that.columnHandle) && Objects.equals(alias, that.alias);
	}

	@Override
	public int hashCode() {
		return Objects.hash(functionName, outputType, columnHandle, alias);
	}

	@Override
	public String toString() {
		return String.format("%s(%s)", functionName, columnHandle.map(RediSearchColumnHandle::getName).orElse(""));
	}
}
