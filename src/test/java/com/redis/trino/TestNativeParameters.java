package com.redis.trino;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.netty.buffer.Unpooled;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;

public class TestNativeParameters {
    private static final MapType TYPE = new MapType(VARCHAR, VARBINARY, new TypeOperators());

    private static SqlMap parameter(String key, Slice value) {
        var builder = TYPE.createBlockBuilder(null, 2);
        builder.buildEntry((keys, values) -> { VARCHAR.writeSlice(keys, utf8Slice("unused")); VARBINARY.writeSlice(values, utf8Slice("old")); });
        builder.buildEntry((keys, values) -> { VARCHAR.writeSlice(keys, utf8Slice(key)); if (value == null) { values.appendNull(); } else { VARBINARY.writeSlice(values, value); } });
        return TYPE.getObject(builder.build(), 1);
    }

    @Test
    void testSqlMapOffsetNullsNamesAndImmutability() {
        Slice binary = Slices.wrappedBuffer(new byte[] {0, (byte) 255, 1, 0});
        Map<String, String> parameters = RediSearchNativeSearch.parseParameters(parameter("blob", binary));
        assertThat(parameters).containsExactlyEntriesOf(Map.of("blob", "AP8BAA=="));
        binary.setByte(0, 99);
        assertThat(parameters.get("blob")).isEqualTo("AP8BAA==");
        assertThatThrownBy(() -> parameters.put("another", "AA==")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> RediSearchNativeSearch.parseParameters(null)).hasMessageContaining("non-null MAP");
        assertThatThrownBy(() -> RediSearchNativeSearch.parseParameters(parameter("blob", null))).hasMessageContaining("must not be null");
        assertThatThrownBy(() -> RediSearchNativeSearch.parseParameters(parameter("", utf8Slice("x")))).hasMessageContaining("non-empty");
        assertThatThrownBy(() -> RediSearchNativeSearch.parseParameters(parameter("a\0b", utf8Slice("x")))).hasMessageContaining("without NUL");
        var builder = TYPE.createBlockBuilder(null, 1);
        builder.buildEntry((keys, values) -> {
            for (int i = 0; i < 2; i++) { VARCHAR.writeSlice(keys, utf8Slice("duplicate")); VARBINARY.writeSlice(values, utf8Slice("x")); }
        });
        assertThatThrownBy(() -> RediSearchNativeSearch.parseParameters(TYPE.getObject(builder.build(), 0)))
                .hasMessageContaining("unique");
    }

    @Test
    void testBinaryWireAndRedactedDiagnostics() {
        byte[] vector = {0, 0, (byte) 144, 63, 0, 0, 0, 0};
        String encoded = Base64.getEncoder().encodeToString(vector);
        RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("tpch", "vectors"), "vectors")
                .withNativeQuery("*=>[KNN 2 @vec $blob]", Map.of("blob", encoded));
        var aggregation = new RediSearchTranslator(new RediSearchConfig()).aggregate(table, List.of(), Optional.empty());
        assertThat(aggregation.getCommandArguments()).contains("PARAMS", "blob", "<redacted binary parameter: 8 bytes>")
                .doesNotContain(encoded);
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
        aggregation.getArgs().build(args);
        var buffer = Unpooled.buffer();
        try {
            args.encode(buffer);
            byte[] wire = new byte[buffer.readableBytes()];
            buffer.readBytes(wire);
            assertThat(wire).containsSubsequence(vector);
        } finally { buffer.release(); }
        assertThat(table.toString()).contains("blob").doesNotContain(encoded);
    }

    @Test
    void testGlobalRankedPhaseGuards() {
        RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("tpch", "vectors"), "vectors")
                .withNativeQuery("*=>[KNN 2 @vec $blob]", Map.of("blob", "AAAAAAAAAAA="));
        RediSearchColumnHandle id = new RediSearchColumnHandle("id", DoubleType.DOUBLE, RediSearchFieldType.NUMERIC, false, true, Optional.empty());
        assertThat(RediSearchScanPartition.isDocumentScan(table)).isFalse();
        assertThat(RediSearchSplitManager.acceptsDynamicFilter(table, List.of(id))).isFalse();
        var constrained = table.withConstraint(TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(DoubleType.DOUBLE, 3.0))));
        assertThatThrownBy(() -> new RediSearchTranslator(new RediSearchConfig()).aggregate(constrained, List.of(id), Optional.empty()))
                .hasMessageContaining("before ranking");
    }
}
