package com.redis.trino;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPlanDiagnostics {
    @Test
    void testConfigurationCapabilityAndPrecisionPoliciesAreDistinct() {
        var config = new RediSearchConfig().setAggregationPushdownEnabled(false).setAggregationGroupLimit(3);
        var policy = RediSearchPlanDiagnostics.capture(config, OptionalLong.of(1050), false, Optional.of(false));
        assertThat(policy).containsEntry("aggregation", "disabled_by_configuration")
                .containsEntry("floating_aggregates", "blocked_by_resp2_precision")
                .containsEntry("normalization_case", "capability_unavailable")
                .containsEntry("group_bound", "finite_type_or_exact_source_domain_proof_required")
                .containsEntry("cached_documents", "1050");
        assertThat(RediSearchPlanDiagnostics.capture(config, OptionalLong.empty(), true, Optional.empty()))
                .containsEntry("cached_documents", "unknown").containsEntry("normalization_case", "not_yet_probed")
                .containsEntry("integer_sum_rows", "unknown_count_requires_trino");
        assertThat(RediSearchPlanDiagnostics.capture(config, OptionalLong.of(4194304), true, Optional.of(true)))
                .containsEntry("integer_sum_rows", "precision_limit_requires_trino")
                .containsEntry("smallint_sum_rows", "cached_count_fits");
        assertThat(RediSearchPlanDiagnostics.capture(config.setFlexEnabled(true), OptionalLong.of(1), true, Optional.empty()))
                .containsEntry("aggregation", "disabled_on_flex").containsEntry("local_topn", "disabled_on_flex");
        assertThatThrownBy(() -> policy.put("aggregation", "changed")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testPolicySerializationEveryCopyAndLegacyDefault() throws Exception {
        JsonCodec<RediSearchTableHandle> codec = JsonCodec.jsonCodec(RediSearchTableHandle.class);
        var input = new HashMap<>(Map.of("aggregation", "disabled_by_configuration"));
        var table = new RediSearchTableHandle(new SchemaTableName("tpch", "table"), "table")
                .withNativeQuery("@name:$blob", Map.of("blob", "c2VjcmV0"))
                .withPushdownPolicy(input);
        input.put("aggregation", "changed");
        var restored = codec.fromJson(codec.toJson(table));
        assertThat(restored).isEqualTo(table).hasSameHashCodeAs(table);
        for (var copy : List.of(restored.withConstraint(TupleDomain.all()), restored.withLimit(1),
                restored.withAggregations(List.of(), List.of()), restored.withAggregationStages(List.of()),
                restored.appendAggregation(List.of(), List.of()), restored.withTopN(List.of(), 1).withoutTopN(),
                restored.withFilters(Map.of()), restored.withNativeQuery("*"), restored.withNativeQuery("*", Map.of()))) {
            assertThat(copy.getPushdownPolicy()).containsEntry("aggregation", "disabled_by_configuration");
        }
        assertThat(restored.toString()).contains("pushdownPolicy", "global_parameterized_query").doesNotContain("c2VjcmV0", "secret");
        var legacy = (com.fasterxml.jackson.databind.node.ObjectNode) new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(codec.toJson(new RediSearchTableHandle(new SchemaTableName("tpch", "old"), "old")));
        legacy.remove("pushdownPolicy");
        assertThat(codec.fromJson(legacy.toString()).getPushdownPolicy()).isEmpty();
        assertThatThrownBy(() -> restored.getPushdownPolicy().put("new", "value")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testAppliedPolicyDistinguishesConnectorFromServerSort() {
        var table = new RediSearchTableHandle(new SchemaTableName("tpch", "table"), "table");
        assertThat(RediSearchPlanDiagnostics.applied(table.withTopN(List.of(new RediSearchSortItem("key", true, true)), 5)))
                .containsEntry("ordering", "connector_typed_topn").containsEntry("limit", "5");
        assertThat(RediSearchPlanDiagnostics.applied(table.withTopN(List.of(new RediSearchSortItem("key", true)), 5)))
                .containsEntry("ordering", "redis_numeric_sort");
    }
}
