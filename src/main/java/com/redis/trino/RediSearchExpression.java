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

import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

/**
 * Arithmetic on the DOUBLE values of NUMERIC columns, which Trino pushes down as a column of its own
 * ({@link RediSearchColumnHandle#getExpression}): a scan computes it in the connector, and an aggregation of it in
 * Redis, with an APPLY step. Only doubles are computed the same way by both: integer arithmetic overflows in SQL but
 * not in Redis, which divides integers as doubles, and computes REAL values as doubles.
 */
public final class RediSearchExpression {

	public enum Operator {
		ADD("+"), SUBTRACT("-"), MULTIPLY("*"), DIVIDE("/");

		private final String symbol;

		Operator(String symbol) {
			this.symbol = symbol;
		}

		double apply(double left, double right) {
			return switch (this) {
			case ADD -> left + right;
			case SUBTRACT -> left - right;
			case MULTIPLY -> left * right;
			case DIVIDE -> left / right;
			};
		}
	}

	private static final Map<FunctionName, Operator> OPERATORS = Map.of(ADD_FUNCTION_NAME, Operator.ADD,
			SUBTRACT_FUNCTION_NAME, Operator.SUBTRACT, MULTIPLY_FUNCTION_NAME, Operator.MULTIPLY, DIVIDE_FUNCTION_NAME,
			Operator.DIVIDE);

	// Exact as doubles below 2^53, and Redis parses larger ones as Java casts them, to the nearest double
	private static final Set<Type> INTEGER_TYPES = Set.of(BIGINT, INTEGER, SMALLINT, TINYINT);

	private final Optional<String> column;
	private final Optional<Type> columnType;
	private final Optional<Double> constant;
	private final Optional<Operator> operator;
	private final List<RediSearchExpression> arguments;

	@JsonCreator
	public RediSearchExpression(@JsonProperty("column") Optional<String> column,
			@JsonProperty("columnType") Optional<Type> columnType, @JsonProperty("constant") Optional<Double> constant,
			@JsonProperty("operator") Optional<Operator> operator,
			@JsonProperty("arguments") List<RediSearchExpression> arguments) {
		this.column = requireNonNull(column, "column is null");
		this.columnType = requireNonNull(columnType, "columnType is null");
		this.constant = requireNonNull(constant, "constant is null");
		this.operator = requireNonNull(operator, "operator is null");
		this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
	}

	static RediSearchExpression column(String name, Type type) {
		return new RediSearchExpression(Optional.of(name), Optional.of(type), Optional.empty(), Optional.empty(),
				List.of());
	}

	static RediSearchExpression constant(double value) {
		return new RediSearchExpression(Optional.empty(), Optional.empty(), Optional.of(value), Optional.empty(),
				List.of());
	}

	static RediSearchExpression operation(Operator operator, RediSearchExpression left, RediSearchExpression right) {
		return new RediSearchExpression(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(operator),
				List.of(left, right));
	}

	/**
	 * The expression for a DOUBLE projection Trino pushes down: arithmetic on DOUBLE NUMERIC columns, integer NUMERIC
	 * columns cast to DOUBLE, and finite DOUBLE constants. Empty for anything else.
	 *
	 * @param assignments the scan's columns, by the names of the variables the projection refers to them by
	 */
	public static Optional<RediSearchExpression> translate(ConnectorExpression expression,
			Map<String, ColumnHandle> assignments) {
		if (!DOUBLE.equals(expression.getType())) {
			return Optional.empty();
		}
		if (expression instanceof Variable variable) {
			RediSearchColumnHandle column = (RediSearchColumnHandle) assignments.get(variable.getName());
			if (column == null) {
				return Optional.empty();
			}
			if (column.getExpression().isPresent()) {
				return column.getExpression();
			}
			return isOperand(column) && column.getType().equals(DOUBLE)
					? Optional.of(column(column.getName(), DOUBLE))
					: Optional.empty();
		}
		if (expression instanceof Constant constant) {
			return constant.getValue() instanceof Double value && Double.isFinite(value)
					? Optional.of(constant(value))
					: Optional.empty();
		}
		if (!(expression instanceof Call call)) {
			return Optional.empty();
		}
		List<ConnectorExpression> arguments = call.getArguments();
		if (call.getFunctionName().equals(CAST_FUNCTION_NAME) && arguments.size() == 1
				&& arguments.get(0) instanceof Variable variable
				&& assignments.get(variable.getName()) instanceof RediSearchColumnHandle column && isOperand(column)
				&& INTEGER_TYPES.contains(column.getType())) {
			return Optional.of(column(column.getName(), column.getType()));
		}
		if (call.getFunctionName().equals(NEGATE_FUNCTION_NAME) && arguments.size() == 1) {
			// Redis has no unary minus. Multiplying by -1 negates exactly, 0 and nan included.
			return translate(arguments.get(0), assignments)
					.map(operand -> operation(Operator.MULTIPLY, operand, constant(-1)));
		}
		Operator operator = OPERATORS.get(call.getFunctionName());
		if (operator == null || arguments.size() != 2) {
			return Optional.empty();
		}
		Optional<RediSearchExpression> left = translate(arguments.get(0), assignments);
		Optional<RediSearchExpression> right = translate(arguments.get(1), assignments);
		if (left.isEmpty() || right.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(operation(operator, left.get(), right.get()));
	}

	// An indexed NUMERIC field, which an expression can refer to as @name
	private static boolean isOperand(RediSearchColumnHandle column) {
		return column.getFieldType() == RediSearchFieldType.NUMERIC && column.isSupportsPredicates()
				&& column.getExpression().isEmpty() && RediSearchQueryBuilder.isProperty(column.getName());
	}

	@JsonProperty
	public Optional<String> getColumn() {
		return column;
	}

	@JsonProperty
	public Optional<Type> getColumnType() {
		return columnType;
	}

	@JsonProperty
	public Optional<Double> getConstant() {
		return constant;
	}

	@JsonProperty
	public Optional<Operator> getOperator() {
		return operator;
	}

	@JsonProperty
	public List<RediSearchExpression> getArguments() {
		return arguments;
	}

	/**
	 * @return the columns the expression reads, and their types, in the order they first appear
	 */
	@JsonIgnore
	public Map<String, Type> getColumns() {
		Map<String, Type> columns = new LinkedHashMap<>();
		addColumns(columns);
		return columns;
	}

	private void addColumns(Map<String, Type> columns) {
		column.ifPresent(name -> columns.putIfAbsent(name, columnType.orElseThrow()));
		arguments.forEach(argument -> argument.addColumns(columns));
	}

	/**
	 * @return the expression as an APPLY step writes it, e.g. {@code (@quantity * @extendedprice)}
	 */
	public String toRedis() {
		if (column.isPresent()) {
			return "@" + column.get();
		}
		if (constant.isPresent()) {
			String value = Double.toString(constant.get());
			return constant.get() < 0 ? "(" + value + ")" : value;
		}
		return "(" + arguments.get(0).toRedis() + " " + operator.orElseThrow().symbol + " " + arguments.get(1).toRedis()
				+ ")";
	}

	/**
	 * @param values the value of each column the expression reads, as the connector reads it, or null for none
	 * @return the result as Java writes a double, or null if a column has no value
	 */
	public String evaluate(Function<String, String> values) {
		Double result = compute(values);
		return result == null ? null : Double.toString(result);
	}

	private Double compute(Function<String, String> values) {
		if (column.isPresent()) {
			String value = values.apply(column.get());
			// An integer column's value as Trino casts it, to the nearest double
			return value == null ? null : Double.parseDouble(value);
		}
		if (constant.isPresent()) {
			return constant.get();
		}
		Double left = arguments.get(0).compute(values);
		if (left == null) {
			return null;
		}
		Double right = arguments.get(1).compute(values);
		if (right == null) {
			return null;
		}
		return operator.orElseThrow().apply(left, right);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RediSearchExpression that = (RediSearchExpression) o;
		return column.equals(that.column) && columnType.equals(that.columnType) && constant.equals(that.constant)
				&& operator.equals(that.operator) && arguments.equals(that.arguments);
	}

	@Override
	public int hashCode() {
		return Objects.hash(column, columnType, constant, operator, arguments);
	}

	@Override
	public String toString() {
		return toRedis();
	}
}
