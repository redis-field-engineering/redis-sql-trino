package com.redis.trino;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestDomainGroupingBounds {
    @Test
    void testInclusiveSingletonSetsAndNullableBoundaries() {
        var key = column("label", VARCHAR);
        Domain two = Domain.multipleValues(VARCHAR, List.of(utf8Slice("Wheat"), utf8Slice("麥 ")));
        assertThat(bound(List.of(key), Map.of(key, two), 2)).hasValue(2);
        assertThat(bound(List.of(key), Map.of(key, two), 1)).isEmpty();
        Domain nullable = Domain.create(two.getValues(), true);
        assertThat(bound(List.of(key), Map.of(key, nullable), 3)).hasValue(3);
        assertThat(bound(List.of(key), Map.of(key, nullable), 2)).isEmpty();
        assertThat(bound(List.of(key), Map.of(key, Domain.onlyNull(VARCHAR)), 1)).hasValue(1);
        assertThat(bound(List.of(key), Map.of(key, Domain.none(VARCHAR)), 1)).hasValue(0);
        assertThat(bound(List.of(key), Map.of(key, nullable), 0)).isEmpty();
    }

    @Test
    void testRangesComplementsAndUnnormalizedNumericKeysRemainUnknown() {
        var key = column("label", VARCHAR);
        Domain single = Domain.singleValue(VARCHAR, utf8Slice("one"));
        assertThat(bound(List.of(key), Map.of(key, single.complement()), 100)).isEmpty();
        assertThat(bound(List.of(key), Map.of(key, Domain.notNull(VARCHAR)), 100)).isEmpty();
        Domain range = Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("a"), true, utf8Slice("z"), true)), false);
        assertThat(bound(List.of(key), Map.of(key, range), 100)).isEmpty();
        var integer = column("number", INTEGER);
        assertThat(bound(List.of(integer), Map.of(integer, Domain.singleValue(INTEGER, 0L)), 1)).isEmpty();
        var floating = column("floating", DOUBLE);
        assertThat(bound(List.of(floating), Map.of(floating, Domain.singleValue(DOUBLE, -0.0)), 1)).hasValue(1);
    }

    @Test
    void testIntrinsicAndDocumentBoundsTakeMinimumAndProductsStayCapped() {
        var flag = column("flag", BOOLEAN);
        var label = column("label", VARCHAR);
        Domain two = Domain.multipleValues(VARCHAR, List.of(utf8Slice("a"), utf8Slice("b")));
        assertThat(bound(List.of(flag, label), Map.of(label, two), 6)).hasValue(6);
        assertThat(bound(List.of(flag, label), Map.of(label, two), 5)).isEmpty();
        assertThat(bound(List.of(flag, label), Map.of(flag, Domain.singleValue(BOOLEAN, true), label, two), 2)).hasValue(2);
        assertThat(bound(List.of(label, label), Map.of(label, two), 2)).hasValue(2);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.of(1050), List.of(label),
                TupleDomain.withColumnDomains(Map.of(label, two)), 2000)).hasValue(2);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.of(1), List.of(label),
                TupleDomain.withColumnDomains(Map.of(label, two)), 2)).hasValue(1);
        var keys = IntStream.range(0, 64).mapToObj(i -> column("k" + i, VARCHAR)).toList();
        Map<ColumnHandle, Domain> domains = new java.util.HashMap<>();
        keys.forEach(key -> domains.put(key, two));
        assertThat(bound(keys, domains, Long.MAX_VALUE)).isEmpty();
        assertThat(bound(List.of(flag, label), Map.of(label, Domain.none(VARCHAR)), 1)).hasValue(0);
    }

    private static OptionalLong bound(List<? extends ColumnHandle> keys, Map<ColumnHandle, Domain> domains, long budget) {
        return RediSearchGroupingBounds.upperBoundWithin(keys, TupleDomain.withColumnDomains(domains), budget);
    }

    private static RediSearchColumnHandle column(String name, Type type) {
        return new RediSearchColumnHandle(name, type, RediSearchSession.toFieldType(type), false, true, Optional.empty());
    }
}
