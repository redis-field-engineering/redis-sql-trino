package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;

public class TestTableHandle {

	private final JsonCodec<RediSearchTableHandle> codec = JsonCodec.jsonCodec(RediSearchTableHandle.class);

	@Test
	public void testRoundTrip() {
		RediSearchTableHandle expected = new RediSearchTableHandle(new SchemaTableName("schema", "table"), "table");

		String json = codec.toJson(expected);
		RediSearchTableHandle actual = codec.fromJson(json);

		assertThat(actual.getSchemaTableName()).isEqualTo(expected.getSchemaTableName());
	}
}
