package com.redis.trino;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import static io.trino.spi.type.BooleanType.BOOLEAN;

/** Finite bounds for keys whose canonical server representation has a proven SQL domain. */
final class RediSearchGroupingBounds {
    private RediSearchGroupingBounds() {}

    /** Combines a cached document count with independent finite-domain proofs; the count is not a snapshot. */
    static OptionalLong upperBoundWithin(OptionalLong documents, List<? extends ColumnHandle> keys, long budget) {
        return upperBoundWithin(documents, keys, TupleDomain.all(), budget);
    }

    static OptionalLong upperBoundWithin(OptionalLong documents, List<? extends ColumnHandle> keys,
            TupleDomain<ColumnHandle> sourceDomains, long budget) {
        if (budget < 1) {
            return OptionalLong.empty();
        }
        OptionalLong keysBound = upperBoundWithin(keys, sourceDomains, budget);
        if (documents.isPresent() && documents.orElseThrow() >= 0 && documents.orElseThrow() <= budget) {
            return OptionalLong.of(keysBound.isPresent() ? Math.min(documents.orElseThrow(), keysBound.orElseThrow())
                    : documents.orElseThrow());
        }
        return keysBound;
    }

    /**
     * Returns the exact product of conservative domain bounds only when it fits the supplied budget.
     * Missing values are included. Normal grouping eligibility/capability checks must still run afterwards.
     * For these keys, any preserved source-presence flags are determined by the canonical key's NULLness,
     * so the flags used by DISTINCT stages cannot increase the number of groups.
     */
    static OptionalLong upperBoundWithin(List<? extends ColumnHandle> keys, long budget) {
        return upperBoundWithin(keys, TupleDomain.all(), budget);
    }

    /** Source domains must be enforced before GROUPBY; residual predicates are not cardinality proofs. */
    static OptionalLong upperBoundWithin(List<? extends ColumnHandle> keys, TupleDomain<ColumnHandle> sourceDomains,
            long budget) {
        if (budget < 1) {
            return OptionalLong.empty();
        }
        if (sourceDomains.isNone()) {
            return OptionalLong.of(0);
        }
        Map<ColumnHandle, Domain> domains = sourceDomains.getDomains().orElseThrow();
        long bound = 1;
        Set<ColumnHandle> seen = new HashSet<>();
        for (ColumnHandle handle : keys) {
            if (!seen.add(handle)) {
                continue;
            }
            if (!(handle instanceof RediSearchColumnHandle column)
                    || column.isHidden() || RediSearchAggregationTerm.fromColumnHandle(column).isEmpty()) {
                return OptionalLong.empty();
            }
            long cardinality = Long.MAX_VALUE;
            if (column.getType().equals(BOOLEAN)) {
                // Canonical true, false (including non-true spellings), and missing/NULL.
                cardinality = 3;
            }
            else if (column.getType() instanceof CharType character && character.getLength() == 0) {
                // Every present value truncates to the empty string; missing stays NULL.
                cardinality = 2;
            }
            Domain domain = domains.get(column);
            // VARCHAR has its stored string representation. Normalized keys also collapse equivalent raw
            // spellings (notably DOUBLE signed zero); unnormalized numeric keys cannot use this proof.
            if (domain != null && domain.getType().equals(column.getType())
                    && (column.getType() instanceof VarcharType
                            || RediSearchGroupingNormalizer.requiresNormalization(column.getType()))) {
                long finite = domain.getValues().isNone() ? 0
                        : domain.getValues().isDiscreteSet() ? domain.getValues().getDiscreteSet().size() : Long.MAX_VALUE;
                if (finite != Long.MAX_VALUE && domain.isNullAllowed()) {
                    finite++;
                }
                cardinality = Math.min(cardinality, finite);
            }
            if (cardinality == Long.MAX_VALUE) {
                return OptionalLong.empty();
            }
            if (cardinality == 0) { return OptionalLong.of(0); }
            // Cap before multiplication, including when budget is Long.MAX_VALUE.
            if (bound > budget / cardinality) {
                return OptionalLong.empty();
            }
            bound *= cardinality;
        }
        return OptionalLong.of(bound);
    }
}
