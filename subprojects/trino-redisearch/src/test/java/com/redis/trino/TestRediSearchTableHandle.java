package com.redis.trino;

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestRediSearchTableHandle
{
    private final JsonCodec<RediSearchTableHandle> codec = JsonCodec.jsonCodec(RediSearchTableHandle.class);

    @Test
    public void testRoundTrip()
    {
    	RediSearchTableHandle expected = new RediSearchTableHandle(new SchemaTableName("schema", "table"));

        String json = codec.toJson(expected);
        RediSearchTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
    }
}
