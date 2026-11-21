package com.redis.trino;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestGroupingNormalizer {
    @Test
    void testAliasesCannotShadowSourceOrEarlierStageFields() {
        Set<String> reserved = new HashSet<>(Set.of("__trino_group0", "__trino_group0_", "metric"));
        var terms = List.of(new RediSearchAggregationTerm("flag", BOOLEAN), new RediSearchAggregationTerm("amount", DOUBLE));
        var first = RediSearchGroupingNormalizer.plan(terms, reserved);
        assertThat(reserved).containsExactlyInAnyOrder("__trino_group0", "__trino_group0_", "metric");
        assertThat(first.aliases()).containsKeys("flag", "amount");
        assertThat(first.beforeExpressions().keySet()).doesNotContainAnyElementsOf(reserved);
        assertThat(first.beforeExpressions().keySet()).containsExactlyInAnyOrderElementsOf(first.aliases().values());
        assertThat(first.afterExpressions()).containsKeys("flag", "amount");
        reserved.addAll(first.beforeExpressions().keySet());
        var second = RediSearchGroupingNormalizer.plan(terms, reserved);
        assertThat(second.beforeExpressions().keySet()).doesNotContainAnyElementsOf(reserved);
        assertThatThrownBy(() -> first.aliases().put("other", "unsafe")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testUnsafeFieldNamesCannotBecomeApplyExpressions() {
        RediSearchColumnHandle quoted = new RediSearchColumnHandle("flag with space", BOOLEAN,
                RediSearchFieldType.TAG, false, true, Optional.empty());
        assertThat(RediSearchAggregationTerm.fromColumnHandle(quoted)).isEmpty();
        assertThatThrownBy(() -> RediSearchGroupingNormalizer.plan(
                List.of(new RediSearchAggregationTerm("flag with space", BOOLEAN)), Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBoundedCharactersAndOrderedIntermediateDependencies() {
        for (int length : new int[] {0, 8}) {
            var type = io.trino.spi.type.CharType.createCharType(length);
            assertThat(RediSearchAggregationTerm.fromColumnHandle(new RediSearchColumnHandle("c", type,
                    RediSearchFieldType.TAG, false, true, Optional.empty()))).isPresent();
            Set<String> available = new HashSet<>(Set.of("c", "__trino_group0_offset0", "__trino_group0_end0"));
            var plan = RediSearchGroupingNormalizer.plan(List.of(new RediSearchAggregationTerm("c", type)), available);
            for (var step : plan.beforeExpressions().entrySet()) {
                var fields = java.util.regex.Pattern.compile("@([A-Za-z_][A-Za-z_0-9]*)").matcher(step.getValue());
                while (fields.find()) {
                    assertThat(available).as("Field referenced before definition: %s", step).contains(fields.group(1));
                }
                assertThat(available.add(step.getKey())).isTrue();
            }
            assertThat(plan.beforeExpressions()).hasSize(2 * length + 1);
        }
        assertThat(RediSearchAggregationTerm.fromColumnHandle(new RediSearchColumnHandle("c", io.trino.spi.type.CharType.createCharType(9),
                RediSearchFieldType.TAG, false, true, Optional.empty()))).isEmpty();
    }

    @Test
    void testUnindexedBooleanCanBeLoadedAndCanonicalized() {
        RediSearchColumnHandle column = new RediSearchColumnHandle("flag", BOOLEAN,
                RediSearchFieldType.TAG, false, false, Optional.empty());
        assertThat(RediSearchAggregationTerm.fromColumnHandle(column)).isPresent();
    }
}
