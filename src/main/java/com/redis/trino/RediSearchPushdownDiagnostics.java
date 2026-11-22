package com.redis.trino;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

/** Stateless eligibility diagnostics: a rejected optimizer attempt must not mutate a shared table handle. */
final class RediSearchPushdownDiagnostics {
    private RediSearchPushdownDiagnostics() {}

    enum Category { CONFIGURATION_DISABLED, CAPABILITY_UNAVAILABLE, CORRECTNESS_GUARD }

    enum Reason {
        AGGREGATION_DISABLED(Category.CONFIGURATION_DISABLED, "Enable redisearch.aggregation-pushdown.enabled to consider eligible aggregates."),
        FLEX_MODE(Category.CAPABILITY_UNAVAILABLE, "Flex uses cursor-free reads with Trino aggregation/ordering; use the supported RAM path for these optimizations."),
        INPUT_ORDERING(Category.CORRECTNESS_GUARD, "Aggregate the unlimited source, or retain Trino aggregation after the existing LIMIT/ORDER BY."),
        MULTIPLE_GROUPING_SETS(Category.CAPABILITY_UNAVAILABLE, "Retain Trino execution for multiple grouping sets."),
        GROUP_COUNT_UNKNOWN(Category.CORRECTNESS_GUARD, "Refresh FT.INFO statistics or use an independently proven finite grouping domain; a selective residual is not a group bound."),
        GROUP_COUNT_INVALID(Category.CORRECTNESS_GUARD, "Refresh invalid FT.INFO statistics; a negative count cannot prove the group budget."),
        GROUP_BUDGET_EXCEEDED(Category.CORRECTNESS_GUARD, "Retain Trino grouping or use an exact finite-domain bound; do not raise the connector budget above the server group limit."),
        UNSUPPORTED_AGGREGATE(Category.CAPABILITY_UNAVAILABLE, "Retain Trino aggregation for this expression, type, FILTER or aggregate shape."),
        CONDITIONAL_JSON(Category.CAPABILITY_UNAVAILABLE, "Retain Trino conditional aggregation for JSON extraction semantics."),
        INTEGER_COUNT_UNKNOWN(Category.CORRECTNESS_GUARD, "Refresh FT.INFO statistics; integer SUM/COUNT precision needs a known whole-index row bound."),
        INTEGER_PRECISION(Category.CORRECTNESS_GUARD, "Retain Trino computation: selective estimates and cancellation cannot establish exact Redis intermediate sums."),
        CASE_EXPRESSION_UNAVAILABLE(Category.CAPABILITY_UNAVAILABLE, "Use a server with a successfully probed case() expression, or retain Trino grouping."),
        JSON_GROUP_KEY(Category.CAPABILITY_UNAVAILABLE, "Retain Trino grouping for JSON keys requiring HASH canonical text decoding."),
        UNSAFE_GROUP_KEY(Category.CORRECTNESS_GUARD, "Retain Trino grouping until this key type and representation have an exact server normalization."),
        GROUPING_ONLY_UNSUPPORTED(Category.CAPABILITY_UNAVAILABLE, "Retain Trino DISTINCT for this source or empty grouping shape."),
        RESP2_FLOATING_POINT(Category.CORRECTNESS_GUARD, "Use RESP3 on every Redis connection, or retain Trino floating aggregation/grouping."),
        DISTINCT_REWRITE_SHAPE(Category.CAPABILITY_UNAVAILABLE, "Retain Trino aggregation unless the input is one grouping-only DISTINCT stage followed by eligible counts."),
        DISTINCT_KEY_MISMATCH(Category.CORRECTNESS_GUARD, "Retain Trino aggregation: second-stage keys must preserve the first-stage key identity and SQL type."),
        TOPN_INPUT_SHAPE(Category.CORRECTNESS_GUARD, "Retain Trino ordering after existing aggregation/ordering/limit, or use a positive limit with stored sort keys."),
        TOPN_SCHEMA_UNSUPPORTED(Category.CAPABILITY_UNAVAILABLE, "Use supported HASH fields or validated modern JSON mappings, or retain Trino ordering."),
        TOPN_SORT_KEY_UNSUPPORTED(Category.CAPABILITY_UNAVAILABLE, "Retain Trino ordering for computed or unavailable sort keys."),
        LOCAL_TOPN_DISABLED(Category.CONFIGURATION_DISABLED, "Enable redisearch.local-topn.max-candidates with a positive bounded budget to consider exact candidate selection."),
        TOPN_CANDIDATE_BUDGET(Category.CORRECTNESS_GUARD, "Retain Trino ordering or explicitly size redisearch.local-topn.max-candidates for the requested limit and available query memory."),
        TOPN_TYPE_UNSUPPORTED(Category.CAPABILITY_UNAVAILABLE, "Retain Trino ordering for sort types without an exact local comparator.");

        private final Category category;
        private final String action;

        Reason(Category category, String action) {
            this.category = category;
            this.action = action;
        }

        Category category() { return category; }
        String action() { return action; }
    }

    record Rejection(Reason reason, String detail) {
        String message(String operation, String index) {
            return "Redis " + operation + " pushdown rejected [category=" + reason.category() + " reason=" + reason
                    + "] index=" + index + " detail=" + detail + " action=" + reason.action();
        }
    }

    static Optional<Rejection> configuration(boolean enabled, boolean flex) {
        if (!enabled) {
            return Optional.of(new Rejection(Reason.AGGREGATION_DISABLED, "redisearch.aggregation-pushdown.enabled=false"));
        }
        if (flex) {
            return Optional.of(new Rejection(Reason.FLEX_MODE, "redisearch.flex.enabled=true"));
        }
        return Optional.empty();
    }

    static Optional<Rejection> groupBudget(OptionalLong documents, List<? extends ColumnHandle> keys,
            TupleDomain<ColumnHandle> exactSourceDomains, long budget) {
        if (RediSearchGroupingBounds.upperBoundWithin(documents, keys, exactSourceDomains, budget).isPresent()) {
            return Optional.empty();
        }
        Reason reason = documents.isEmpty() ? Reason.GROUP_COUNT_UNKNOWN
                : documents.orElseThrow() < 0 ? Reason.GROUP_COUNT_INVALID : Reason.GROUP_BUDGET_EXCEEDED;
        return Optional.of(new Rejection(reason, "cachedWholeIndexDocuments=" + documents + " groupBudget=" + budget
                + " finiteDomainProofWithinBudget=unavailable; cached counts and selectivity estimates are not snapshots"));
    }

    static Optional<Rejection> integerPrecision(RediSearchAggregation aggregation, OptionalLong documents) {
        if (aggregation.isIntegerSumSafe(documents)) {
            return Optional.empty();
        }
        return Optional.of(new Rejection(documents.isEmpty() ? Reason.INTEGER_COUNT_UNKNOWN : Reason.INTEGER_PRECISION,
                "cachedWholeIndexDocuments=" + documents + " safeRowLimit=" + aggregation.getIntegerSumRowLimit()));
    }

    static Optional<Rejection> floatingTransport(boolean resp3, boolean floatingOutput, boolean floatingKeys) {
        return !resp3 && (floatingOutput || floatingKeys)
                ? Optional.of(new Rejection(Reason.RESP2_FLOATING_POINT,
                        "protocol=RESP2 floatingOutput=" + floatingOutput + " floatingGroupingKey=" + floatingKeys))
                : Optional.empty();
    }

    static Optional<Rejection> localTopNBudget(long requested, int budget) {
        if (budget == 0) {
            return Optional.of(new Rejection(Reason.LOCAL_TOPN_DISABLED, "redisearch.local-topn.max-candidates=0"));
        }
        return requested > budget ? Optional.of(new Rejection(Reason.TOPN_CANDIDATE_BUDGET,
                "requestedCandidates=" + requested + " candidateBudget=" + budget)) : Optional.empty();
    }
}
