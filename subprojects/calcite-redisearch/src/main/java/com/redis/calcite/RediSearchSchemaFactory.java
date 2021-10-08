package com.redis.calcite;

import com.redis.lettucemod.RedisModulesClient;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.Map;


/**
 * Factory that creates a {@link RediSearchSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class RediSearchSchemaFactory implements SchemaFactory {

    public RediSearchSchemaFactory() {
        // Do Nothing
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        Map map = (Map) operand;
        String uri = (String) map.get("uri");
        String index = (String) map.get("index");
        RedisModulesClient client = RedisModulesClient.create(uri);
        return new RediSearchSchema(client.connect(), Arrays.asList(index));
    }
}
