package com.redis.trino;

import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

public class TestExpression {

	private static final Map<String, ColumnHandle> ASSIGNMENTS = Map.of("q", numeric("quantity", DOUBLE), "p",
			numeric("extendedprice", DOUBLE), "n", numeric("count", BIGINT), "r", numeric("rate", REAL), "s",
			new RediSearchColumnHandle("style", VARCHAR, RediSearchFieldType.TAG, false, true, Optional.empty()), "h",
			numeric("brewery-id", DOUBLE), "u",
			new RediSearchColumnHandle("unindexed", DOUBLE, RediSearchFieldType.NUMERIC, false, false, Optional.empty()));

	@Test
	public void testTranslate() {
		assertThat(translate(call(MULTIPLY_FUNCTION_NAME, variable("q"), variable("p"))))
				.contains("(@quantity * @extendedprice)");
		// Redis has no unary minus
		assertThat(translate(call(NEGATE_FUNCTION_NAME, variable("q")))).contains("(@quantity * (-1.0))");
		// An integer cast to a double, which Redis parses the same
		assertThat(translate(call(ADD_FUNCTION_NAME, call(CAST_FUNCTION_NAME, DOUBLE, new Variable("n", BIGINT)),
				new Constant(1.5, DOUBLE)))).contains("(@count + 1.5)");
		assertThat(translate(call(DIVIDE_FUNCTION_NAME, variable("q"), new Constant(1e-5, DOUBLE))))
				.contains("(@quantity / 1.0E-5)");
		// A pushed-down column's expression
		RediSearchExpression product = RediSearchExpression
				.translate(call(MULTIPLY_FUNCTION_NAME, variable("q"), variable("p")), ASSIGNMENTS).orElseThrow();
		Map<String, ColumnHandle> assignments = new HashMap<>(ASSIGNMENTS);
		assignments.put("expr", RediSearchColumnHandle.expression(product));
		assertThat(RediSearchExpression.translate(call(ADD_FUNCTION_NAME, variable("expr"), variable("q")), assignments)
				.map(RediSearchExpression::toRedis)).contains("((@quantity * @extendedprice) + @quantity)");
	}

	@Test
	public void testNotTranslated() {
		// Integer arithmetic overflows in SQL but not in Redis
		assertThat(RediSearchExpression.translate(new Call(BIGINT, MULTIPLY_FUNCTION_NAME,
				List.of(new Variable("n", BIGINT), new Variable("n", BIGINT))), ASSIGNMENTS)).isEmpty();
		// Trino computes REAL values as floats, and Redis parses them from text as doubles
		assertThat(translate(call(CAST_FUNCTION_NAME, DOUBLE, new Variable("r", REAL)))).isEmpty();
		assertThat(translate(call(MULTIPLY_FUNCTION_NAME, variable("q"), new Constant(Double.NaN, DOUBLE)))).isEmpty();
		// Fields an expression can't refer to
		assertThat(translate(call(MULTIPLY_FUNCTION_NAME, variable("q"), variable("h")))).isEmpty();
		assertThat(translate(call(MULTIPLY_FUNCTION_NAME, variable("q"), variable("u")))).isEmpty();
		assertThat(translate(call(MULTIPLY_FUNCTION_NAME, variable("q"), variable("missing")))).isEmpty();
	}

	@Test
	public void testEvaluate() {
		RediSearchExpression expression = RediSearchExpression.translate(call(ADD_FUNCTION_NAME,
				call(MULTIPLY_FUNCTION_NAME, variable("q"), variable("p")),
				call(CAST_FUNCTION_NAME, DOUBLE, new Variable("n", BIGINT))), ASSIGNMENTS).orElseThrow();
		assertThat(expression.getColumns()).containsExactly(Map.entry("quantity", DOUBLE),
				Map.entry("extendedprice", DOUBLE), Map.entry("count", BIGINT));
		Map<String, String> values = new HashMap<>(
				Map.of("quantity", "0.1", "extendedprice", "3", "count", "9007199254740993"));
		assertThat(expression.evaluate(values::get)).isEqualTo(Double.toString(0.1 * 3 + (double) 9007199254740993L));
		values.remove("count");
		assertThat(expression.evaluate(values::get)).isNull();
		values.put("count", "0");
		values.put("quantity", "1.7976931348623157E308");
		assertThat(expression.evaluate(values::get)).isEqualTo("Infinity");
	}

	private static Optional<String> translate(ConnectorExpression expression) {
		return RediSearchExpression.translate(expression, ASSIGNMENTS).map(RediSearchExpression::toRedis);
	}

	private static Call call(io.trino.spi.expression.FunctionName name, ConnectorExpression... arguments) {
		return call(name, DOUBLE, arguments);
	}

	private static Call call(io.trino.spi.expression.FunctionName name, Type type, ConnectorExpression... arguments) {
		return new Call(type, name, List.of(arguments));
	}

	private static Variable variable(String name) {
		return new Variable(name, ((RediSearchColumnHandle) ASSIGNMENTS.get(name)) == null ? DOUBLE
				: ((RediSearchColumnHandle) ASSIGNMENTS.get(name)).getType());
	}

	private static RediSearchColumnHandle numeric(String name, Type type) {
		return new RediSearchColumnHandle(name, type, RediSearchFieldType.NUMERIC, false, true, Optional.empty());
	}
}
