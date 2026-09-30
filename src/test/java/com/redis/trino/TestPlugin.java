package com.redis.trino;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.google.common.collect.ImmutableMap;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;

@TestInstance(PER_CLASS)
public class TestPlugin {

	private RediSearchServer server;

	@BeforeAll
	public void start() {
		server = new RediSearchServer();
	}

	@Test
	public void testCreateConnector() {
		RediSearchPlugin plugin = new RediSearchPlugin();

		ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
		Connector connector = factory.create("test", ImmutableMap.of("redisearch.uri", server.getRedisURI()),
				new TestingConnectorContext());

		assertThat(plugin.getTypes()).isEmpty();

		connector.shutdown();
	}

	@AfterAll
	public void destroy() {
		if (server != null) {
			server.close();
		}
	}
}
