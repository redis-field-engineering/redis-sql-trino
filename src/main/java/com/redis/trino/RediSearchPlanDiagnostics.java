package com.redis.trino;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Immutable planning facts, never a mutable log of optimizer attempts. No query/parameter values are included. */
final class RediSearchPlanDiagnostics {
    private RediSearchPlanDiagnostics() {}

    static RediSearchTableHandle annotate(RediSearchTable table, RediSearchSession session) {
        return table.getTableHandle().withPushdownPolicy(capture(session.getConfig(), table.getIndexInfo().getNumDocs(),
                session.isResp3(), session.getCachedCaseSupport()));
    }

    static Map<String, String> capture(RediSearchConfig config, OptionalLong documents, boolean resp3,
            Optional<Boolean> cachedCaseSupport) {
        Map<String, String> policy = new LinkedHashMap<>();
        policy.put("snapshot", "planning_policy_not_rejection_history");
        policy.put("reader", config.isFlexEnabled() ? "flex_keyset" : "redis_cursor");
        policy.put("aggregation", config.isFlexEnabled() ? "disabled_on_flex"
                : config.isAggregationPushdownEnabled() ? "enabled_subject_to_expression_safety" : "disabled_by_configuration");
        policy.put("floating_aggregates", resp3 ? "resp3_transport" : "blocked_by_resp2_precision");
        policy.put("normalization_case", cachedCaseSupport.map(value -> value ? "available" : "capability_unavailable")
                .orElse("not_yet_probed"));
        policy.put("cached_documents", documents.isPresent() && documents.orElseThrow() >= 0
                ? Long.toString(documents.orElseThrow()) : "unknown");
        policy.put("group_budget", Long.toString(config.getAggregationGroupLimit()));
        policy.put("group_bound", RediSearchMetadata.isGroupPushdownSafe(documents, config.getAggregationGroupLimit())
                ? "document_count_fits" : "finite_type_or_exact_source_domain_proof_required");
        policy.put("local_topn", config.isFlexEnabled() ? "disabled_on_flex"
                : config.getLocalTopNMaxCandidates() == 0 ? "disabled_by_configuration" : "typed_candidate_selection");
        policy.put("local_topn_candidate_budget", Integer.toString(config.getLocalTopNMaxCandidates()));
        integerSum(policy, "tinyint", 128, documents);
        integerSum(policy, "smallint", 32768, documents);
        integerSum(policy, "integer", 2147483648L, documents);
        return Collections.unmodifiableMap(policy);
    }

    private static void integerSum(Map<String, String> policy, String type, long magnitude, OptionalLong documents) {
        long limit = ((1L << 53) - 1) / magnitude;
        policy.put(type + "_sum_row_limit", Long.toString(limit));
        policy.put(type + "_sum_rows", documents.isEmpty() || documents.orElseThrow() < 0 ? "unknown_count_requires_trino"
                : documents.orElseThrow() <= limit ? "cached_count_fits" : "precision_limit_requires_trino");
    }

    static Map<String, String> applied(RediSearchTableHandle table) {
        Map<String, String> applied = new LinkedHashMap<>();
        applied.put("indexed_constraint", table.getConstraint().isAll() ? "none" : table.getConstraint().isNone() ? "empty" : "present");
        applied.put("loaded_filters", Integer.toString(table.getFilters().size()));
        applied.put("aggregation_stages", Integer.toString(table.getAggregationStages().size()));
        applied.put("ordering", table.getSort().isEmpty() ? "trino_or_unordered"
                : table.getSort().stream().anyMatch(RediSearchSortItem::isLocal) ? "connector_typed_topn" : "redis_numeric_sort");
        applied.put("limit", table.getLimit().isPresent() ? Long.toString(table.getLimit().orElseThrow()) : "none");
        applied.put("native_selection", table.hasNativeParameters() ? "global_parameterized_query"
                : table.getNativeQuery().isPresent() ? "native_query" : "none");
        return Collections.unmodifiableMap(applied);
    }
}
