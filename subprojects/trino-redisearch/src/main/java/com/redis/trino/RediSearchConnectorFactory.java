package com.redis.trino;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import com.google.inject.Injector;

import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.type.TypeManager;

public class RediSearchConnectorFactory implements ConnectorFactory {

	@Override
	public String getName() {
		return "redisearch";
	}

	@Override
	public ConnectorHandleResolver getHandleResolver() {
		return new RediSearchHandleResolver();
	}

	@Override
	public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
		requireNonNull(config, "config is null");

		Bootstrap app = new Bootstrap(new JsonModule(), new RediSearchClientModule(),
				binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()));

		Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

		return injector.getInstance(RediSearchConnector.class);
	}
}
