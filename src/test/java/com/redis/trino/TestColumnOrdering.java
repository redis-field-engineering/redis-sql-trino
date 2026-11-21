package com.redis.trino;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

class TestColumnOrdering {
    @Test
    void partialTypeMetadataDoesNotDeclareExternalColumnOrder() {
        var columns = List.of(column("a"), column("b"), column("c"), RediSearchBuiltinField.KEY.getColumnHandle());
        assertThat(RediSearchColumnTypes.orderColumns(columns, Map.of("c", "bigint"), Set.of("a", "b", "c")))
                .isEqualTo(columns);
        assertThat(RediSearchColumnTypes.orderColumns(columns, Map.of("b", "varchar"), Set.of("a", "b")))
                .isEqualTo(columns);
    }

    @Test
    void declaredOrderSurvivesAnAdditionalUnindexedField() {
        Map<String, String> types = new LinkedHashMap<>();
        types.put("b", "varchar");
        types.put("a", "varchar");
        var key = RediSearchBuiltinField.KEY.getColumnHandle();
        assertThat(RediSearchColumnTypes.orderColumns(List.of(column("a"), column("b"), column("extra"), key),
                types, Set.of("a", "b")))
                .containsExactly(column("b"), column("a"), column("extra"), key);
    }

    private static RediSearchColumnHandle column(String name) {
        return new RediSearchColumnHandle(name, VARCHAR, RediSearchFieldType.TAG, false, true, Optional.empty());
    }
}
