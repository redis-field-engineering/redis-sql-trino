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

import java.util.List;

import javax.inject.Inject;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;

public class RediSearchPageSinkProvider implements ConnectorPageSinkProvider {

	private final RediSearchSession session;

	@Inject
	public RediSearchPageSinkProvider(RediSearchSession rediSearchSession) {
		this.session = rediSearchSession;
	}

	@Override
	public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId) {
		RediSearchOutputTableHandle handle = (RediSearchOutputTableHandle) outputTableHandle;
		return pageSink(handle.getSchemaTableName(), handle.getColumns());
	}

	@Override
	public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId) {
		RediSearchInsertTableHandle handle = (RediSearchInsertTableHandle) insertTableHandle;
		return pageSink(handle.getSchemaTableName(), handle.getColumns());
	}

	private RediSearchPageSink pageSink(SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns) {
		return new RediSearchPageSink(session, schemaTableName, columns);
	}
}
