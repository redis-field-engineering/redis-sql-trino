package com.redis.trino;

import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.CharType;

import static io.trino.spi.type.BooleanType.BOOLEAN;

/** Finite bounds for keys whose canonical server representation has a proven SQL domain. */
final class RediSearchGroupingBounds {
    private RediSearchGroupingBounds() {}

    /** Uses a trustworthy document count first, otherwise an independent finite-domain proof. */
    static OptionalLong upperBoundWithin(OptionalLong documents, List<? extends ColumnHandle> keys, long budget) {
        if (budget < 1) {
            return OptionalLong.empty();
        }
        if (documents.isPresent() && documents.orElseThrow() >= 0 && documents.orElseThrow() <= budget) {
            return documents;
        }
        return upperBoundWithin(keys, budget);
    }

    /**
     * Returns the exact product of conservative domain bounds only when it fits the supplied budget.
     * Missing values are included. Normal grouping eligibility/capability checks must still run afterwards.
     * For these keys, any preserved source-presence flags are determined by the canonical key's NULLness,
     * so the flags used by DISTINCT stages cannot increase the number of groups.
     */
    static OptionalLong upperBoundWithin(List<? extends ColumnHandle> keys, long budget) {
        if (budget < 1) {
            return OptionalLong.empty();
        }
        long bound = 1;
        Set<ColumnHandle> seen = new HashSet<>();
        for (ColumnHandle handle : keys) {
            if (!seen.add(handle)) {
                continue;
            }
            if (!(handle instanceof RediSearchColumnHandle column)
                    || RediSearchAggregationTerm.fromColumnHandle(column).isEmpty()
                    || !RediSearchGroupingNormalizer.requiresNormalization(column.getType())) {
                return OptionalLong.empty();
            }
            long cardinality;
            if (column.getType().equals(BOOLEAN)) {
                // Canonical true, false (including non-true spellings), and missing/NULL.
                cardinality = 3;
            }
            else if (column.getType() instanceof CharType character && character.getLength() == 0) {
                // Every present value truncates to the empty string; missing stays NULL.
                cardinality = 2;
            }
            else {
                return OptionalLong.empty();
            }
            // Cap before multiplication, including when budget is Long.MAX_VALUE.
            if (bound > budget / cardinality) {
                return OptionalLong.empty();
            }
            bound *= cardinality;
        }
        return OptionalLong.of(bound);
    }
}
