package com.redis.trino;

import com.google.common.collect.ImmutableList;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

public class RediSearchPlugin implements Plugin {

	@Override
	public Iterable<ConnectorFactory> getConnectorFactories() {
		return ImmutableList.of(new RediSearchConnectorFactory());
	}
}
