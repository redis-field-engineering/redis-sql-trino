package com.redis.trino;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.lettuce.core.search.FieldValue;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import static com.redis.trino.RediSearchPushdownDiagnostics.Category.CAPABILITY_UNAVAILABLE;
import static com.redis.trino.RediSearchPushdownDiagnostics.Category.CONFIGURATION_DISABLED;
import static com.redis.trino.RediSearchPushdownDiagnostics.Category.CORRECTNESS_GUARD;
import static com.redis.trino.RediSearchPushdownDiagnostics.Reason.GROUP_BUDGET_EXCEEDED;
import static com.redis.trino.RediSearchPushdownDiagnostics.Reason.GROUP_COUNT_INVALID;
import static com.redis.trino.RediSearchPushdownDiagnostics.Reason.GROUP_COUNT_UNKNOWN;
import static com.redis.trino.RediSearchPushdownDiagnostics.Reason.INTEGER_COUNT_UNKNOWN;
import static com.redis.trino.RediSearchPushdownDiagnostics.Reason.INTEGER_PRECISION;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPushdownDiagnostics {
    @Test
    void testConfigurationAndUnavailableCapabilityHaveDifferentActions() {
        var disabled = RediSearchPushdownDiagnostics.configuration(false, false).orElseThrow();
        assertThat(disabled.reason().category()).isEqualTo(CONFIGURATION_DISABLED);
        assertThat(disabled.message("aggregation", "test_index"))
                .contains("category=CONFIGURATION_DISABLED", "reason=AGGREGATION_DISABLED", "index=test_index",
                        "action=Enable redisearch.aggregation-pushdown.enabled");
        var flex = RediSearchPushdownDiagnostics.configuration(true, true).orElseThrow();
        assertThat(flex.reason().category()).isEqualTo(CAPABILITY_UNAVAILABLE);
        assertThat(flex.message("aggregation", "test_index")).contains("reason=FLEX_MODE", "Trino aggregation/ordering");
        assertThat(RediSearchPushdownDiagnostics.configuration(true, false)).isEmpty();
        assertThat(RediSearchPushdownDiagnostics.configuration(false, true).orElseThrow().reason())
                .isEqualTo(RediSearchPushdownDiagnostics.Reason.AGGREGATION_DISABLED);
    }

    @Test
    void testResp2AndResp3FloatingTransportMatrix() {
        for (boolean output : List.of(false, true)) {
            for (boolean keys : List.of(false, true)) {
                assertThat(RediSearchPushdownDiagnostics.floatingTransport(true, output, keys)).isEmpty();
                var resp2 = RediSearchPushdownDiagnostics.floatingTransport(false, output, keys);
                assertThat(resp2.isPresent()).isEqualTo(output || keys);
                resp2.ifPresent(rejection -> {
                    assertThat(rejection.reason().category()).isEqualTo(CORRECTNESS_GUARD);
                    assertThat(rejection.message("aggregation", "i"))
                            .contains("reason=RESP2_FLOATING_POINT", "protocol=RESP2", "Use RESP3 on every Redis connection");
                });
            }
        }
    }

    @Test
    void testMissingInvalidAndExcessiveWholeIndexCountsAreDistinguished() {
        List<ColumnHandle> keys = List.of(column("label", VARCHAR));
        var unknown = RediSearchPushdownDiagnostics.groupBudget(OptionalLong.empty(), keys, TupleDomain.all(), 3).orElseThrow();
        assertThat(unknown.reason()).isEqualTo(GROUP_COUNT_UNKNOWN);
        assertThat(unknown.message("aggregation", "i")).contains("selective residual is not a group bound");
        assertThat(RediSearchPushdownDiagnostics.groupBudget(OptionalLong.of(-1), keys, TupleDomain.all(), 3)
                .orElseThrow().reason()).isEqualTo(GROUP_COUNT_INVALID);
        assertThat(RediSearchPushdownDiagnostics.groupBudget(OptionalLong.of(4), keys, TupleDomain.all(), 3)
                .orElseThrow().reason()).isEqualTo(GROUP_BUDGET_EXCEEDED);
        assertThat(RediSearchPushdownDiagnostics.groupBudget(OptionalLong.of(3), keys, TupleDomain.all(), 3)).isEmpty();
    }

    @Test
    void testExactSourceKeyDomainsCanProveGroupsButOtherSelectiveFiltersCannot() {
        var label = column("label", VARCHAR);
        var filter = column("filter", VARCHAR);
        TupleDomain<ColumnHandle> selectedKey = TupleDomain.withColumnDomains(Map.of(label,
                Domain.singleValue(VARCHAR, utf8Slice("private source literal"))));
        assertThat(RediSearchPushdownDiagnostics.groupBudget(OptionalLong.empty(), List.of(label), selectedKey, 1)).isEmpty();
        TupleDomain<ColumnHandle> selectedOtherField = TupleDomain.withColumnDomains(Map.of(filter,
                Domain.singleValue(VARCHAR, utf8Slice("private source literal"))));
        var rejected = RediSearchPushdownDiagnostics.groupBudget(OptionalLong.of(1050), List.of(label), selectedOtherField, 1)
                .orElseThrow();
        assertThat(rejected.reason()).isEqualTo(GROUP_BUDGET_EXCEEDED);
        assertThat(rejected.message("aggregation", "i")).doesNotContain("private source literal", "filter");
        // A residual SELECTIVITY estimate supplies no exact source domain at all.
        assertThat(RediSearchPushdownDiagnostics.groupBudget(OptionalLong.of(1050), List.of(label), TupleDomain.all(), 1))
                .isPresent();
    }

    @Test
    void testFiniteGroupingProofNeverReplacesWholeIndexIntegerPrecisionBound() {
        var flag = column("flag", BOOLEAN);
        var sum = integerSum();
        long safe = sum.getIntegerSumRowLimit().orElseThrow();
        for (OptionalLong documents : List.of(OptionalLong.empty(), OptionalLong.of(safe + 1))) {
            assertThat(RediSearchPushdownDiagnostics.groupBudget(documents, List.of(flag), TupleDomain.all(), 3)).isEmpty();
            assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, documents)).isPresent();
        }
        assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, OptionalLong.empty()).orElseThrow().reason())
                .isEqualTo(INTEGER_COUNT_UNKNOWN);
        assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, OptionalLong.of(safe + 1)).orElseThrow().reason())
                .isEqualTo(INTEGER_PRECISION);
        assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, OptionalLong.of(safe))).isEmpty();
        assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, OptionalLong.of(-1))).isPresent();
    }

    @Test
    void testStalePlanningCountDoesNotPermitUnsafeOutputAfterIndexGrowth() {
        var sum = integerSum();
        long safe = sum.getIntegerSumRowLimit().orElseThrow();
        // Cached metadata is not a snapshot: diagnostics cannot know that growth has happened.
        assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, OptionalLong.of(safe))).isEmpty();
        var reader = new RediSearchRowReader(List.of("total"), Map.of(), Map.of(), Set.of(), List.of(sum), Map.of());
        assertThatThrownBy(() -> reader.read(Map.of("total", value("0"), sum.getCountAlias(), value("1"),
                sum.integerSumCountAlias(), value(Long.toString(safe + 1)))))
                .hasMessageContaining("no longer exact after index growth");
        assertThat(RediSearchPushdownDiagnostics.integerPrecision(sum, OptionalLong.of(safe + 1)).orElseThrow().reason())
                .isEqualTo(INTEGER_PRECISION);
    }

    @Test
    void testDisabledLocalTopNAndCandidateBudgetHaveDifferentActions() {
        var disabled = RediSearchPushdownDiagnostics.localTopNBudget(5, 0).orElseThrow();
        assertThat(disabled.reason().category()).isEqualTo(CONFIGURATION_DISABLED);
        assertThat(disabled.message("TopN", "i")).contains("reason=LOCAL_TOPN_DISABLED", "positive bounded budget");
        var overBudget = RediSearchPushdownDiagnostics.localTopNBudget(1001, 1000).orElseThrow();
        assertThat(overBudget.reason().category()).isEqualTo(CORRECTNESS_GUARD);
        assertThat(overBudget.message("TopN", "i")).contains("reason=TOPN_CANDIDATE_BUDGET", "requestedCandidates=1001");
        assertThat(RediSearchPushdownDiagnostics.localTopNBudget(1000, 1000)).isEmpty();
    }

    private static RediSearchAggregation integerSum() {
        return new RediSearchAggregation(RediSearchAggregation.SUM, BIGINT,
                Optional.of(RediSearchColumnHandle.integerWidening(column("small", SMALLINT), BIGINT)), "total", true);
    }

    private static RediSearchColumnHandle column(String name, Type type) {
        return new RediSearchColumnHandle(name, type, RediSearchSession.toFieldType(type), false, true, Optional.empty());
    }

    private static FieldValue value(String value) {
        return FieldValue.of(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
