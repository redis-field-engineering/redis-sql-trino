/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
