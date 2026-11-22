package com.redis.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Variable;

class TestCountExpressionEligibility {
    private static Optional<RediSearchAggregation> count(RediSearchColumnHandle column) {
        return RediSearchAggregation.handleAggregation(new AggregateFunction("count", BIGINT,
                List.of(new Variable("x", column.getType())), List.of(), false, Optional.empty()), Map.of("x", column), "c", false);
    }

    @Test
    void testCountCompilesOnlyDeduplicatedOperandPresence() {
        var d = RediSearchExpression.column("d", DOUBLE);
        var expression = RediSearchExpression.operation(RediSearchExpression.Operator.ADD,
                RediSearchExpression.operation(RediSearchExpression.Operator.DIVIDE, d, RediSearchExpression.column("e", DOUBLE)), d);
        var metric = count(RediSearchColumnHandle.expression(expression)).orElseThrow();
        assertThat(metric.getInputFields()).containsExactly("d", "e");
        assertThat(metric.hasValueExpression()).isEqualTo("exists(@d) && exists(@e)");
        var table = new RediSearchTableHandle(new SchemaTableName("tpch", "numbers"), "numbers")
                .withAggregations(List.of(), List.of(metric));
        var output = new RediSearchColumnHandle("c", BIGINT, RediSearchFieldType.NUMERIC, false, false, Optional.empty());
        String command = String.join(" ", new RediSearchTranslator(new RediSearchConfig()).aggregate(table, List.of(output), Optional.empty()).getCommandArguments());
        assertThat(command).contains("exists(@d) && exists(@e)").doesNotContain(" / ", "__expr");
    }

    @Test
    void testUnrepresentableSourceNamesAndConstantProjectionsFallBack() {
        assertThat(count(RediSearchColumnHandle.expression(RediSearchExpression.constant(1)))).isEmpty();
        assertThat(count(RediSearchColumnHandle.expression(RediSearchExpression.column("bad.name", DOUBLE)))).isEmpty();
    }

    @Test
    void testCountKeepsExactIndicatorSumBound() {
        var metric = count(RediSearchColumnHandle.expression(RediSearchExpression.column("d", DOUBLE))).orElseThrow();
        long bound = (1L << 53) - 1;
        assertThat(metric.getIntegerSumRowLimit()).hasValue(bound);
        assertThat(metric.isIntegerSumSafe(OptionalLong.of(bound))).isTrue();
        assertThat(metric.isIntegerSumSafe(OptionalLong.of(bound + 1))).isFalse();
        assertThat(metric.isIntegerSumSafe(OptionalLong.empty())).isFalse();
    }
}
