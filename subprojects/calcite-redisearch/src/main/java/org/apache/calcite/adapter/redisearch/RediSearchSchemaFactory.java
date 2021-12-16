package org.apache.calcite.adapter.redisearch;

import java.util.Map;
import java.util.Objects;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.base.Strings;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.RedisURI;

/**
 * Factory that creates a {@link RediSearchSchema}.
 */
public class RediSearchSchemaFactory implements SchemaFactory {

	@Override
	public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
		Map<String, Object> map = operand;
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
