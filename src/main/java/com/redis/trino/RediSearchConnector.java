/*
 * MIT License
 *
 * Copyright (c) 2022, Redis Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.redis.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

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
	public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
		RediSearchMetadata metadata = transactions.get(transactionHandle);
		checkTransaction(metadata, transactionHandle);
		return metadata;
	}

	private void checkTransaction(Object object, ConnectorTransactionHandle transactionHandle) {
		checkArgument(object != null, "no such transaction: %s", transactionHandle);
	}

	@Override
	public void commit(ConnectorTransactionHandle transaction) {
		checkTransaction(transactions.remove(transaction), transaction);
	}

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
