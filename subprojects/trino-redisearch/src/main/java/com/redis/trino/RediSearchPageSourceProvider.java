package com.redis.trino;

import com.google.common.collect.ImmutableList;
import com.redis.trino.RediSearchTableHandle.Type;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RediSearchPageSourceProvider implements ConnectorPageSourceProvider {
	private final RediSearchSession rediSearchSession;

	@Inject
	public RediSearchPageSourceProvider(RediSearchSession rediSearchSession) {
		this.rediSearchSession = requireNonNull(rediSearchSession, "rediSearchSession is null");
	}

	@Override
	public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
			ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
		RediSearchTableHandle tableHandle = (RediSearchTableHandle) table;

		ImmutableList.Builder<RediSearchColumnHandle> handles = ImmutableList.builder();
		for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
			handles.add((RediSearchColumnHandle) handle);
		}
		ImmutableList<RediSearchColumnHandle> columnHandles = handles.build();
		if (tableHandle.getType() == Type.AGGREGATE) {
			return new RediSearchPageSourceAggregate(rediSearchSession, tableHandle, columnHandles);
		}
		return new RediSearchPageSource(rediSearchSession, tableHandle, columnHandles);
	}
}
