package com.redislabs.jdbc.rel;

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

    public static final String REDIS_URI = "uri";
    public static final String INDEXES = "indexes";
    public static final String COMMA_DELIMITER = ",";

    public RediSearchSchemaFactory() {
        // Do Nothing
    }

    @Override
    public synchronized Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String uri = (String) operand.get(REDIS_URI);
        String[] indexNames = ((String) operand.get(INDEXES)).split(COMMA_DELIMITER);
        RedisModulesClient client = RedisModulesClient.create(uri);
        return new RediSearchSchema(client.connect(), Arrays.asList(indexNames));
    }
}
