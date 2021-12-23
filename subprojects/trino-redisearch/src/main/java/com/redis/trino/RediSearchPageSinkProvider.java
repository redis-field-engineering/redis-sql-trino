package com.redis.trino;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

public class RediSearchPageSinkProvider implements ConnectorPageSinkProvider {

	private final RediSearchConfig config;
	private final RediSearchSession rediSearchSession;

	@Inject
	public RediSearchPageSinkProvider(RediSearchConfig config, RediSearchSession rediSearchSession) {
		this.config = config;
		this.rediSearchSession = rediSearchSession;
	}

	@Override
	public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorOutputTableHandle outputTableHandle) {
		RediSearchOutputTableHandle handle = (RediSearchOutputTableHandle) outputTableHandle;
		return new RediSearchPageSink(config, rediSearchSession, handle.getSchemaTableName(), handle.getColumns());
	}

	@Override
	public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorInsertTableHandle insertTableHandle) {
		RediSearchInsertTableHandle handle = (RediSearchInsertTableHandle) insertTableHandle;
		return new RediSearchPageSink(config, rediSearchSession, handle.getSchemaTableName(), handle.getColumns());
	}
}
