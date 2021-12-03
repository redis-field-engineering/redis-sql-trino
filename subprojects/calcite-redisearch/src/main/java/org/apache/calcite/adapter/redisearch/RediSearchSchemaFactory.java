package org.apache.calcite.adapter.redisearch;

import com.google.common.base.Strings;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.RedisURI;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;


/**
 * Factory that creates a {@link RediSearchSchema}.
 */
@SuppressWarnings("UnusedDeclaration")
public class RediSearchSchemaFactory implements SchemaFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(RediSearchSchemaFactory.class);

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        Map map = operand;
        String uri = (String) map.get("uri");
        String index = (String) map.get("index");
        String username = (String) map.get("username");
        String password = (String) map.get("password");
        return new RediSearchSchema(connect(uri, username, password), index);
    }

    private StatefulRedisModulesConnection<String, String> connect(String uri, String username, String password) {
        Objects.requireNonNull(uri, "URI");
        RedisURI redisURI = RedisURI.create(uri);
        if (!Strings.isNullOrEmpty(username) && !Strings.isNullOrEmpty(password)) {
            redisURI.setUsername(username);
            redisURI.setPassword(password.toCharArray());
        }
        RedisModulesClient client = RedisModulesClient.create(redisURI);
        return client.connect();
    }
}
