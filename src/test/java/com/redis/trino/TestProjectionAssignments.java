package com.redis.trino;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableBiMap;
import org.junit.jupiter.api.Test;

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestProjectionAssignments {
    private static final RediSearchTableHandle TABLE = new RediSearchTableHandle(new SchemaTableName("tpch", "source"), "source");

    @Test
    void testNewWideningReusesAHandleProjectedByAnEarlierOptimizerPass() {
        var source = column("v", INTEGER);
        var widening = new Call(BIGINT, CAST_FUNCTION_NAME, List.of(new Variable("v", INTEGER)));
        var first = RediSearchMetadata.projectExpressions(TABLE, List.of(new Variable("v", INTEGER), widening),
                Map.of("v", source)).orElseThrow();
        Map<String, ColumnHandle> existing = assignments(first);
        Variable previous = (Variable) first.getProjections().get(1);
        List<ConnectorExpression> projections = List.of(previous, widening, new Variable("v", INTEGER), widening);

        var result = RediSearchMetadata.projectExpressions(TABLE, projections, existing).orElseThrow();
        assertThat(result.getProjections()).containsExactly(previous, previous, new Variable("v", INTEGER), previous);
        assertThat(assignments(result)).hasSize(2).containsEntry(previous.getName(), RediSearchColumnHandle.integerWidening(source, BIGINT));
        // PushAggregationIntoTableScan constructs this inverse map after projection pushdown.
        assertThat(ImmutableBiMap.copyOf(assignments(result)).inverse()).hasSize(2);
        assertThat(RediSearchMetadata.projectExpressions(TABLE, result.getProjections(), assignments(result))).isEmpty();
    }

    @Test
    void testArithmeticReusesAnExistingProjectedVariableInEitherOutputOrder() {
        var source = column("value", DOUBLE);
        var call = new Call(DOUBLE, MULTIPLY_FUNCTION_NAME, List.of(new Variable("v", DOUBLE), new Constant(2.0, DOUBLE)));
        var expression = RediSearchColumnHandle.expression(RediSearchExpression.translate(call, Map.of("v", source)).orElseThrow());
        Variable existing = new Variable("previous_expression", DOUBLE);
        for (List<ConnectorExpression> outputs : List.of(List.<ConnectorExpression>of(existing, call), List.<ConnectorExpression>of(call, existing))) {
            var result = RediSearchMetadata.projectExpressions(TABLE, outputs,
                    Map.of("v", source, existing.getName(), expression)).orElseThrow();
            assertThat(result.getProjections()).containsExactly(existing, existing);
            assertThat(assignments(result)).containsExactly(Map.entry(existing.getName(), expression));
            assertThat(ImmutableBiMap.copyOf(assignments(result)).inverse()).hasSize(1);
        }
    }

    @Test
    void testEqualNewExpressionsShareOneAssignmentWithoutLosingOriginalOutputs() {
        var source = column("v", INTEGER);
        var other = column("id", BIGINT);
        var cast = new Call(BIGINT, CAST_FUNCTION_NAME, List.of(new Variable("v", INTEGER)));
        var result = RediSearchMetadata.projectExpressions(TABLE,
                List.of(new Variable("id", BIGINT), cast, new Variable("v", INTEGER), cast),
                Map.of("id", other, "v", source)).orElseThrow();
        assertThat(result.getProjections()).hasSize(4);
        assertThat(result.getProjections().get(1)).isEqualTo(result.getProjections().get(3));
        assertThat(result.getProjections().get(0)).isEqualTo(new Variable("id", BIGINT));
        assertThat(result.getProjections().get(2)).isEqualTo(new Variable("v", INTEGER));
        assertThat(ImmutableBiMap.copyOf(assignments(result)).inverse()).hasSize(3);
    }

    @Test
    void testVariableOnlyProjectionDoesNotClaimOptimizerProgress() {
        var source = column("v", INTEGER);
        var widened = RediSearchColumnHandle.integerWidening(source, BIGINT);
        assertThat(RediSearchMetadata.projectExpressions(TABLE,
                List.of(new Variable("v", INTEGER), new Variable("existing", BIGINT)),
                Map.of("v", source, "existing", widened))).isEmpty();
    }

    private static Map<String, ColumnHandle> assignments(ProjectionApplicationResult<ConnectorTableHandle> result) {
        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        for (Assignment assignment : result.getAssignments()) {
            assignments.put(assignment.getVariable(), assignment.getColumn());
        }
        return assignments;
    }

    private static RediSearchColumnHandle column(String name, Type type) {
        return new RediSearchColumnHandle(name, type, RediSearchFieldType.NUMERIC, false, true, Optional.empty());
    }
}
