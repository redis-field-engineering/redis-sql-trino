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
