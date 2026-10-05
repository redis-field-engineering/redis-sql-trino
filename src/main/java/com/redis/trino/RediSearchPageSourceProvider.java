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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Optional;

import com.google.inject.Inject;

import com.google.common.collect.ImmutableList;

import io.airlift.log.Logger;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;

public class RediSearchPageSourceProvider implements ConnectorPageSourceProvider {

	private static final Logger log = Logger.get(RediSearchPageSourceProvider.class);

	// A larger set of join keys becomes the range it spans, which keeps the query short
	private static final int DYNAMIC_FILTER_COMPACTION_THRESHOLD = 256;

	private final RediSearchSession rediSearchSession;

	@Inject
	public RediSearchPageSourceProvider(RediSearchSession rediSearchSession) {
		this.rediSearchSession = requireNonNull(rediSearchSession, "rediSearchSession is null");
	}

	// RediSearchPageSource reports no memory usage, so there is nothing to send to memoryContext
	@Override
	public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
			ConnectorSplit split, ConnectorTableHandle table, Optional<ConnectorTableCredentials> tableCredentials,
			List<ColumnHandle> columns, DynamicFilter dynamicFilter, MemoryContext memoryContext) {
		RediSearchTableHandle tableHandle = (RediSearchTableHandle) table;
		if (rediSearchSession.getConfig().isDynamicFilteringEnabled()
				&& RediSearchSplitManager.acceptsDynamicFilter(tableHandle, dynamicFilter.getColumnsCovered())) {
			tableHandle = withDynamicFilter(tableHandle, dynamicFilter.getCurrentPredicate());
			if (tableHandle.getConstraint().isNone()) {
				// No row can match a join key
				return new EmptyPageSource();
			}
		}
		ImmutableList.Builder<RediSearchColumnHandle> handles = ImmutableList.builder();
		for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
			handles.add((RediSearchColumnHandle) handle);
		}
		ImmutableList<RediSearchColumnHandle> columnHandles = handles.build();
		return new RediSearchPageSource(rediSearchSession, tableHandle, columnHandles);
	}

	/**
	 * The table, with the dynamic filter's domains that Redis can evaluate added to its query. Its query may return
	 * rows outside them, such as a TAG query's case-insensitive matches, which the join leaves out.
	 */
	private RediSearchTableHandle withDynamicFilter(RediSearchTableHandle table, TupleDomain<ColumnHandle> dynamicFilter) {
		TupleDomain<ColumnHandle> pushed = dynamicFilter.simplify(DYNAMIC_FILTER_COMPACTION_THRESHOLD)
				.filter((column, domain) -> rediSearchSession.canQuery(table, (RediSearchColumnHandle) column, domain));
		if (pushed.isAll()) {
			return table;
		}
		log.debug("Adding dynamic filter %s to %s", pushed, table);
		return table.withConstraint(table.getConstraint().intersect(pushed));
	}
}
