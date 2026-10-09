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
				.withTopN(List.of(new RediSearchSortItem("price", false, true)), 10).withFilters(java.util.Map.of("url", "contains(@url, \"google\")"));

		String json = codec.toJson(expected);
		RediSearchTableHandle actual = codec.fromJson(json);

		assertThat(actual).isEqualTo(expected);
	}
}
