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

import javax.inject.Inject;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class RediSearchPageSinkProvider implements ConnectorPageSinkProvider {

	private final RediSearchSession rediSearchSession;

	@Inject
	public RediSearchPageSinkProvider(RediSearchSession rediSearchSession) {
		this.rediSearchSession = rediSearchSession;
	}

	@Override
	public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorOutputTableHandle outputTableHandle) {
		RediSearchOutputTableHandle handle = (RediSearchOutputTableHandle) outputTableHandle;
		return new RediSearchPageSink(rediSearchSession, handle.getSchemaTableName(), handle.getColumns());
	}

	@Override
	public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
			ConnectorInsertTableHandle insertTableHandle) {
		RediSearchInsertTableHandle handle = (RediSearchInsertTableHandle) insertTableHandle;
		return new RediSearchPageSink(rediSearchSession, handle.getSchemaTableName(), handle.getColumns());
	}
}
