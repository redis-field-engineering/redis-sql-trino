package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.VarcharType;
import io.trino.type.TypeDeserializer;

public class TestTableHandle {

	private final JsonCodec<RediSearchTableHandle> codec = JsonCodec.jsonCodec(RediSearchTableHandle.class);

	@Test
	public void testRoundTrip() {
		RediSearchTableHandle expected = new RediSearchTableHandle(new SchemaTableName("schema", "table"), "table")
				.withNativeQuery("@title:redis | @title:search")
				.withTopN(List.of(new RediSearchSortItem("price", false, true)), 10).withFilters(java.util.Map.of("url", "contains(@url, \"google\")"));

		String json = codec.toJson(expected);
		RediSearchTableHandle actual = codec.fromJson(json);

		assertThat(actual).isEqualTo(expected);
		assertThat(actual.withoutTopN().getNativeQuery()).contains("@title:redis | @title:search");
		assertThat(actual.withConstraint(io.trino.spi.predicate.TupleDomain.all()).getNativeQuery()).isEqualTo(expected.getNativeQuery());
		assertThat(actual.withLimit(1).getNativeQuery()).isEqualTo(expected.getNativeQuery());
		assertThat(actual.withAggregations(List.of(), List.of()).getNativeQuery()).isEqualTo(expected.getNativeQuery());
	}

	@Test
	public void testAggregationStageRoundTripAndCopies() {
		Map<TypeId, Type> types = Map.of(TypeId.of("varchar"), VarcharType.VARCHAR, TypeId.of("bigint"), BigintType.BIGINT);
		JsonCodec<RediSearchTableHandle> stageCodec = new JsonCodecFactory(new JsonMapperProvider()
				.withJsonDeserializers(Map.of(Type.class, new TypeDeserializer(types::get)))).jsonCodec(RediSearchTableHandle.class);
		RediSearchColumnHandle value = new RediSearchColumnHandle("v", VarcharType.VARCHAR, RediSearchFieldType.TAG,
				false, true, Optional.empty());
		RediSearchAggregationStage first = new RediSearchAggregationStage(List.of(new RediSearchAggregationTerm("v", VarcharType.VARCHAR)),
				List.of(new RediSearchAggregation("count", BigintType.BIGINT, Optional.empty(), "internal")), true);
		RediSearchTableHandle expected = new RediSearchTableHandle(new SchemaTableName("schema", "table"), "table")
				.withNativeQuery("@name:redis").withAggregationStages(List.of(first))
				.appendAggregation(List.of(), List.of(new RediSearchAggregation("count", BigintType.BIGINT, Optional.of(value), "result")));
		RediSearchTableHandle restored = stageCodec.fromJson(stageCodec.toJson(expected));
		assertThat(restored).isEqualTo(expected);
		assertThat(restored.getAggregationStages()).hasSize(2);
		assertThat(restored.getAggregationStages().getFirst().groupingOnly()).isTrue();
		assertThat(restored.withLimit(1).getAggregationStages()).isEqualTo(expected.getAggregationStages());
		assertThat(restored.withFilters(Map.of("v", "exists(@v)")).getAggregationStages()).isEqualTo(expected.getAggregationStages());
		assertThat(restored.withTopN(List.of(new RediSearchSortItem("result", false)), 1).withoutTopN().getAggregationStages())
				.isEqualTo(expected.getAggregationStages());
		assertThat(restored.withConstraint(io.trino.spi.predicate.TupleDomain.all()).getAggregationStages()).isEqualTo(expected.getAggregationStages());
	}
}
