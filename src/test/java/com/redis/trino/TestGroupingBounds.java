package com.redis.trino;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestGroupingBounds {
    @Test
    void testNullableDomainsAndExactBudgetBoundaries() {
        var flag = column("flag", BOOLEAN);
        var zero = column("zero", CharType.createCharType(0));
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag), 3)).hasValue(3);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag), 2)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(zero), 2)).hasValue(2);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(zero), 1)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag, zero), 6)).hasValue(6);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag, zero), 5)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag, flag, zero), 6)).hasValue(6);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag, column("other", BOOLEAN)), 9)).hasValue(9);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(flag, column("other", BOOLEAN)), 8)).isEmpty();
    }

    @Test
    void testUnknownOrUnnormalizedKeysNeverProduceAnOptimisticBound() {
        for (var unknown : List.of(column("label", VARCHAR), column("c", CharType.createCharType(1)), column("small", TINYINT))) {
            assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(column("flag", BOOLEAN), unknown), Long.MAX_VALUE)).isEmpty();
        }
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(column("unsafe name", BOOLEAN)), 3)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(new ColumnHandle() {}), Long.MAX_VALUE)).isEmpty();
    }

    @Test
    void testUnknownDocumentCountsNeedAnIndependentProof() {
        var finite = List.of(column("flag", BOOLEAN));
        var unknown = List.of(column("label", VARCHAR));
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.empty(), finite, 3)).hasValue(3);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.empty(), finite, 2)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.empty(), unknown, 3)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.of(-1), unknown, 3)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.of(1050), finite, 3)).hasValue(3);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(OptionalLong.of(2), unknown, 3)).hasValue(2);
    }

    @Test
    void testMultiplicationCannotWrapAtLongMaxBudget() {
        List<RediSearchColumnHandle> keys = IntStream.range(0, 40).mapToObj(i -> column("b" + i, BOOLEAN)).toList();
        long thirtyNineKeys = BigInteger.valueOf(3).pow(39).longValueExact();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(keys.subList(0, 39), Long.MAX_VALUE)).hasValue(thirtyNineKeys);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(keys, Long.MAX_VALUE)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(keys.subList(0, 39), thirtyNineKeys - 1)).isEmpty();
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(), 1)).hasValue(1);
        assertThat(RediSearchGroupingBounds.upperBoundWithin(List.of(), 0)).isEmpty();
    }

    private static RediSearchColumnHandle column(String name, Type type) {
        return new RediSearchColumnHandle(name, type, RediSearchSession.toFieldType(type), false, true, Optional.empty());
    }
}
