package com.redis.trino;

import java.util.List;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

public class RediSearchSplitManager implements ConnectorSplitManager {

	private final List<HostAddress> addresses;

	@Inject
	public RediSearchSplitManager(RediSearchSession session) {
		this.addresses = session.getAddresses();
	}

	@Override
	public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
			ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, DynamicFilter dynamicFilter) {
		RediSearchSplit split = new RediSearchSplit(addresses);

		return new FixedSplitSource(ImmutableList.of(split));
	}
}
