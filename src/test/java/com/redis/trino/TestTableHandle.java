package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;

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
}
