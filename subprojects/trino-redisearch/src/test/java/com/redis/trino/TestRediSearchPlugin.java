package com.redis.trino;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;

public class TestRediSearchPlugin {
	private RediSearchServer server;

	@BeforeClass
	public void start() {
		server = new RediSearchServer();
	}

	@Test
	public void testCreateConnector() {
		RediSearchPlugin plugin = new RediSearchPlugin();

		ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
		Connector connector = factory.create("test",
				ImmutableMap.of("redisearch.uri", server.getTestContext().getRedisURI()),
				new TestingConnectorContext());

		assertFalse(plugin.getTypes().iterator().hasNext());

		connector.shutdown();
	}

	@AfterClass(alwaysRun = true)
	public void destroy() {
		server.close();
	}
}
