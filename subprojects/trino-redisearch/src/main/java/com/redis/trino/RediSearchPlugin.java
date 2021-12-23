package com.redis.trino;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class RediSearchPlugin implements Plugin {

	private final ConnectorFactory connectorFactory;

	public RediSearchPlugin() {
		connectorFactory = new RediSearchConnectorFactory();
	}

	@VisibleForTesting
	RediSearchPlugin(RediSearchConnectorFactory factory) {
		connectorFactory = requireNonNull(factory, "factory is null");
	}

	@Override
	public Iterable<ConnectorFactory> getConnectorFactories() {
		return ImmutableList.of(connectorFactory);
	}
}
