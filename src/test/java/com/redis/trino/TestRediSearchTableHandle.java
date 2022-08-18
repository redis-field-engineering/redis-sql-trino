package com.redis.trino;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.redis.trino.RediSearchTableHandle.Type;

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;

public class TestRediSearchTableHandle {
	private final JsonCodec<RediSearchTableHandle> codec = JsonCodec.jsonCodec(RediSearchTableHandle.class);

	@Test
	public void testRoundTrip() {
		RediSearchTableHandle expected = new RediSearchTableHandle(Type.SEARCH, new SchemaTableName("schema", "table"));

		String json = codec.toJson(expected);
		RediSearchTableHandle actual = codec.fromJson(json);

		assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
	}
}
