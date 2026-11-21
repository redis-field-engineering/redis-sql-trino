package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.AclSetuserArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.protocol.CommandType;

/** Real command-ACL verification uses OSS; Enterprise role provisioning is a separate control-plane operation. */
class TestAtomicHashAcl {
	@Test
	void testDeniedDeleteIsDetectedBeforeAnySet() {
		String image = System.getProperty("redisearch.test.oss-image", "redis:8.10.3");
		try (GenericContainer<?> server = new GenericContainer<>(DockerImageName.parse(image)).withExposedPorts(6379)) {
			server.start();
			RedisClient client = RedisClient.create(RedisURI.create(server.getHost(), server.getMappedPort(6379)));
			try (var admin = client.connect(); var restricted = client.connect()) {
				admin.sync().hset("row", Map.of("a", "old", "b", "keep"));
				admin.sync().aclSetuser("sql_limited", new AclSetuserArgs().reset().on().nopass().allKeys().noCommands()
						.addCommand(CommandType.EVAL).addCommand(CommandType.HSET).addCommand(CommandType.HGETALL));
				restricted.sync().auth("sql_limited", "");
				String[] arguments = RediSearchHashUpdate.arguments(Map.of("a", "new", RediSearchSession.ROW_MARKER, "1"), List.of("b"));
				assertThatThrownBy(() -> restricted.sync().eval(RediSearchHashUpdate.SCRIPT, ScriptOutputType.INTEGER,
						new String[] {"row"}, arguments))
						.isInstanceOf(RedisCommandExecutionException.class)
						.hasMessageContaining("NOPERM atomic HASH update requires HDEL permission");
				assertThat(admin.sync().hgetall("row")).containsExactlyInAnyOrderEntriesOf(Map.of("a", "old", "b", "keep"));

				admin.sync().aclSetuser("sql_limited", new AclSetuserArgs().addCommand(CommandType.HDEL));
				assertThat(restricted.sync().<Long>eval(RediSearchHashUpdate.SCRIPT, ScriptOutputType.INTEGER,
						new String[] {"row"}, arguments)).isEqualTo(1L);
				assertThat(admin.sync().hgetall("row"))
						.containsExactlyInAnyOrderEntriesOf(Map.of("a", "new", RediSearchSession.ROW_MARKER, "1"));

				admin.sync().aclSetuser("sql_limited", new AclSetuserArgs().removeCommand(CommandType.EVAL));
				assertThatThrownBy(() -> restricted.sync().eval(RediSearchHashUpdate.SCRIPT, ScriptOutputType.INTEGER,
						new String[] {"row"}, RediSearchHashUpdate.arguments(Map.of("a", "unexpected"), List.of())))
						.isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining("NOPERM");
				assertThat(admin.sync().hget("row", "a")).isEqualTo("new");
			} finally {
				client.shutdown();
			}
		}
	}
}
