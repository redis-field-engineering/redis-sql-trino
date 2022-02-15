package com.redis.trino;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class RediSearchConnector implements Connector {

	private final RediSearchSession rediSearchSession;
	private final RediSearchSplitManager splitManager;
	private final RediSearchPageSourceProvider pageSourceProvider;
	private final RediSearchPageSinkProvider pageSinkProvider;

	private final ConcurrentMap<ConnectorTransactionHandle, RediSearchMetadata> transactions = new ConcurrentHashMap<>();

	@Inject
	public RediSearchConnector(RediSearchSession rediSearchSession, RediSearchSplitManager splitManager,
			RediSearchPageSourceProvider pageSourceProvider, RediSearchPageSinkProvider pageSinkProvider) {
		this.rediSearchSession = rediSearchSession;
		this.splitManager = requireNonNull(splitManager, "splitManager is null");
		this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
		this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
	}

	@Override
	public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly,
			boolean autoCommit) {
		checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
		RediSearchTransactionHandle transaction = new RediSearchTransactionHandle();
		transactions.put(transaction, new RediSearchMetadata(rediSearchSession));
		return transaction;
	}

	@Override
	public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction) {
		RediSearchMetadata metadata = transactions.get(transaction);
		checkTransaction(metadata, transaction);
		return metadata;
	}
	
	private void checkTransaction(Object object, ConnectorTransactionHandle transaction) {
		checkArgument(object != null, "no such transaction: %s", transaction);
	}

	@Override
	public void commit(ConnectorTransactionHandle transaction) {
		checkTransaction(transactions.remove(transaction), transaction);
	}

//	@Override
//	public void rollback(ConnectorTransactionHandle transaction) {
//		RediSearchMetadata metadata = transactions.remove(transaction);
//		checkTransaction(metadata, transaction);
//		metadata.rollback();
//	}

	@Override
	public ConnectorSplitManager getSplitManager() {
		return splitManager;
	}

	@Override
	public ConnectorPageSourceProvider getPageSourceProvider() {
		return pageSourceProvider;
	}

	@Override
	public ConnectorPageSinkProvider getPageSinkProvider() {
		return pageSinkProvider;
	}

	@Override
	public void shutdown() {
		rediSearchSession.shutdown();
	}
}
