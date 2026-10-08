package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;

public class TestScanPartitions {
    private final RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("default", "hits"), "hits");
    private final RediSearchConfig config = new RediSearchConfig().setScanSplits(4).setScanPartitionField("id")
            .setScanPartitionBoundaries(List.of(0.0, 25.0, 50.0));

    private RediSearchIndexInfo info(RediSearchIndexInfo.KeyType type, boolean indexed) {
        return new RediSearchIndexInfo(Optional.of(type), List.of(), List.of(new RediSearchIndexInfo.Field(
                "id", "id", RediSearchFieldType.NUMERIC, Optional.empty(), indexed)), false, 1, false);
    }

    @Test
    public void testAutomaticCountUsesSizeCapacityAndPool() {
        assertThat(RediSearchAutoScanPlanner.splitCount(100, 8, 32)).isEqualTo(1);
        assertThat(RediSearchAutoScanPlanner.splitCount(500_000, 8, 32)).isEqualTo(2);
        assertThat(RediSearchAutoScanPlanner.splitCount(5_000_000, 8, 32)).isEqualTo(8);
        assertThat(RediSearchAutoScanPlanner.splitCount(5_000_000, 8, 2)).isEqualTo(2);
        assertThat(RediSearchAutoScanPlanner.splitCount(5_000_000, 3, 32)).isEqualTo(3);
        assertThat(RediSearchAutoScanPlanner.splitCount(Long.MAX_VALUE, 100, 100)).isEqualTo(64);
    }

    @Test
    public void testPlanAndSerialization() {
        var partitions = RediSearchScanPartition.plan(config, table, info(RediSearchIndexInfo.KeyType.HASH, true));
        assertThat(partitions.stream().map(RediSearchScanPartition::query)).containsExactly(
                "-@id:[0.0 +inf]", "@id:[0.0 +inf] -@id:[25.0 +inf]",
                "@id:[25.0 +inf] -@id:[50.0 +inf]", "@id:[50.0 +inf]");
        assertThat(RediSearchScanPartition.plan(config, table, info(RediSearchIndexInfo.KeyType.JSON, true)))
                .extracting(RediSearchScanPartition::query).containsExactlyElementsOf(partitions.stream().map(RediSearchScanPartition::query).toList());
        var codec = JsonCodec.jsonCodec(RediSearchSplit.class);
        var split = new RediSearchSplit(List.of(), Optional.of(partitions.get(1)));
        assertThat(codec.fromJson(codec.toJson(split)).getPartition()).isEqualTo(split.getPartition());
    }

    @Test
    public void testBoundAndFallback() {
        config.setScanConnections(2);
        assertThat(RediSearchScanPartition.plan(config, table, info(RediSearchIndexInfo.KeyType.HASH, true)))
                .hasSize(2).extracting(RediSearchScanPartition::query)
                .containsExactly("-@id:[25.0 +inf]", "@id:[25.0 +inf]");
        assertThat(RediSearchScanPartition.plan(config, table.withLimit(10), info(RediSearchIndexInfo.KeyType.HASH, true))).isEmpty();
        assertThat(RediSearchScanPartition.plan(config, table.withAggregations(List.of(), List.of(
                new RediSearchAggregation(RediSearchAggregation.COUNT, io.trino.spi.type.BigintType.BIGINT, Optional.empty(), "count"))), info(RediSearchIndexInfo.KeyType.HASH, true))).isEmpty();
        assertThat(RediSearchScanPartition.plan(config, table, info(RediSearchIndexInfo.KeyType.HASH, false))).isEmpty();
        config.setScanPartitionField("missing");
        assertThat(RediSearchScanPartition.plan(config, table, info(RediSearchIndexInfo.KeyType.HASH, true))).isEmpty();
    }

    @Test
    public void testNoIndexFlagParsing() {
        var info = RediSearchIndexInfo.parse(List.of("index_definition", List.of("key_type", "HASH"),
                "attributes", List.of(List.of("identifier", "id", "attribute", "id", "type", "NUMERIC", "SORTABLE", "NOINDEX"))));
        var resp3 = RediSearchIndexInfo.parse(List.of("index_definition", List.of("key_type", "HASH"),
                "attributes", List.of(List.of("identifier", "id", "attribute", "id", "type", "NUMERIC", "flags", List.of("NOINDEX")))));
        assertThat(resp3.getFields().getFirst().isIndexed()).isFalse();
        assertThat(info.getFields().getFirst().isIndexed()).isFalse();
        assertThat(RediSearchScanPartition.plan(config, table, info)).isEmpty();
    }

    @Test
    public void testInvalidBoundaries() {
        for (List<Double> cuts : List.of(List.of(1.0, 1.0), List.of(2.0, 1.0), List.of(Double.NaN), List.of(Double.POSITIVE_INFINITY))) {
            assertThatThrownBy(() -> config.setScanPartitionBoundaries(cuts)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testSchemaChangeFailsAndGlobalOperationsRejected() {
        var partition = new RediSearchScanPartition("id", Optional.empty(), Optional.of(0.0));
        var translator = new RediSearchTranslator(config);
        assertThatThrownBy(() -> translator.aggregate(table, List.of(), Optional.of(info(RediSearchIndexInfo.KeyType.HASH, false)), Optional.of(partition)))
                .hasMessageContaining("no longer indexed NUMERIC");
        assertThatThrownBy(() -> translator.aggregate(table.withLimit(10), List.of(), Optional.of(info(RediSearchIndexInfo.KeyType.HASH, true)), Optional.of(partition)))
                .hasMessageContaining("global operation");
    }
}
