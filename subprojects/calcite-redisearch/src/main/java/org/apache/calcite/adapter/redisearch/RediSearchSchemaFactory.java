package org.apache.calcite.adapter.redisearch;

import com.redis.lettucemod.RedisModulesClient;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;


/**
 * Factory that creates a {@link RediSearchSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class RediSearchSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String uri = (String) operand.get("uri");
        String index = (String) operand.get("index");
        String username = (String) operand.get("username");
        String password = (String) operand.get("password");
        RedisModulesClient client = RedisModulesClient.create(uri);
        return new RediSearchSchema(uri, index, username, password);
    }
}
