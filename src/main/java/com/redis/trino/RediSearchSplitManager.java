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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.inject.Inject;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

public class RediSearchSplitManager implements ConnectorSplitManager {

	private final List<HostAddress> addresses;
	private final RediSearchConfig config;

	@Inject
	public RediSearchSplitManager(RediSearchSession session) {
		this.addresses = session.getAddresses();
		this.config = session.getConfig();
	}

	@Override
	public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
			ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns, Constraint constraint) {
		RediSearchSplit split = new RediSearchSplit(addresses);
		if (!config.isDynamicFilteringEnabled()
				|| !acceptsDynamicFilter((RediSearchTableHandle) table, dynamicFilterColumns)) {
			return new FixedSplitSource(split);
		}
		long waitTimeoutMillis = config.getDynamicFilteringWaitTimeout().toMillis();
		// Trino holds the split back until the dynamic filters are collected, or the wait times out
		return new FixedSplitSource(split) {
			@Override
			public long getRequestedDynamicFilterWaitTimeoutMillis() {
				return waitTimeoutMillis;
			}
		};
	}

	/**
	 * Whether a scan of the table can add a join's dynamic filter on any of the columns to its query: one that reads
	 * documents, rather than groups or a limited number of them, and a column Redis can query.
	 */
	static boolean acceptsDynamicFilter(RediSearchTableHandle table, Collection<ColumnHandle> columns) {
		return table.getLimit().isEmpty() && table.getTermAggregations().isEmpty()
				&& table.getMetricAggregations().isEmpty() && columns.stream().map(RediSearchColumnHandle.class::cast)
						.anyMatch(RediSearchColumnHandle::isSupportsPredicates);
	}
}
