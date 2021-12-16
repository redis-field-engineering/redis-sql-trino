// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.redis.postgredis;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.primitives.Bytes;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.test.Beers;
import com.redis.postgredis.metadata.OptionsMetadata;

@Category(IntegrationTest.class)
@RunWith(JUnit4.class)
public final class ITQueryTest implements IntegrationTest {

	private ProxyServer server;
	private static PgAdapterTestEnv testEnv = new PgAdapterTestEnv();
	private static String[] args;

	@Before
	public void setUp() throws Exception {
		StatefulRedisModulesConnection<String, String> connection = RedisModulesClient.create("redis://localhost:6379")
				.connect();
		if (!connection.sync().list().contains("beers")) {
			Beers.populateIndex(connection);
		}
		testEnv.setUp();
		args = new String[] { "-p", testEnv.getProjectId(), "-i", testEnv.getInstanceId(),
//          "-d",
//          db.getId().getDatabase(),
//          "-c",
//          testEnv.getCredentials(),
				"-s", String.valueOf(testEnv.getPort()) };
	}

	@Before
	public void startServer() throws Exception {
		server = new ProxyServer(new OptionsMetadata(args));
		server.startServer();
	}

	@After
	public void stopServer() throws Exception {
		if (server != null)
			server.stopServer();
	}


//	@Test
	public void simplePgQuery() throws Exception {
		testEnv.waitForServer(server);

		Socket clientSocket = new Socket(InetAddress.getByName(null), testEnv.getPort());
		DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
		DataInputStream in = new DataInputStream(clientSocket.getInputStream());
		testEnv.initializeConnection(out);
		testEnv.consumeStartUpMessages(in);

		// Run a query that is PG specific to ensure this is a PG dialect database.
		String payload = "SELECT 42::int8\0";
		byte[] messageMetadata = { 'Q', 0, 0, 0, (byte) (payload.length() + 4) };
		byte[] message = Bytes.concat(messageMetadata, payload.getBytes());
		out.write(message, 0, message.length);

		// Check for correct message type identifiers. Exactly 1 row should be returned.
		// See https://www.postgresql.org/docs/13/protocol-message-formats.html for more
		// details.
		testEnv.consumePGMessage('T', in);
		PgAdapterTestEnv.PGMessage dataRow = testEnv.consumePGMessage('D', in);
		testEnv.consumePGMessage('C', in);
		testEnv.consumePGMessage('Z', in);

		// Check data returned payload
		// see here: https://www.postgresql.org/docs/13/protocol-message-formats.html
		DataInputStream dataRowIn = new DataInputStream(new ByteArrayInputStream(dataRow.getPayload()));
		// Number of column values.
		assertThat((int) dataRowIn.readShort(), is(equalTo(1)));
		// Column value length (2 bytes expected)
		assertThat(dataRowIn.readInt(), is(equalTo(2)));
		// Value of the column: '42'
		assertThat(dataRowIn.readByte(), is(equalTo((byte) '4')));
		assertThat(dataRowIn.readByte(), is(equalTo((byte) '2')));
	}

	@Test
	public void basicSelectTest() throws Exception {
		testEnv.waitForServer(server);

		Socket clientSocket = new Socket(InetAddress.getByName(null), testEnv.getPort());
		DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
		DataInputStream in = new DataInputStream(clientSocket.getInputStream());
		testEnv.initializeConnection(out);
		testEnv.consumeStartUpMessages(in);

		// Send query.
		String payload = "SELECT id, name FROM redisearch.beers LIMIT 3\0";
		byte[] messageMetadata = { 'Q', 0, 0, 0, (byte) (payload.length() + 4) };
		byte[] message = Bytes.concat(messageMetadata, payload.getBytes());
		out.write(message, 0, message.length);

		// Check for correct message type identifiers.
		// See https://www.postgresql.org/docs/13/protocol-message-formats.html for more
		// details.
		PgAdapterTestEnv.PGMessage rowDescription = testEnv.consumePGMessage('T', in);
		PgAdapterTestEnv.PGMessage[] dataRows = { testEnv.consumePGMessage('D', in), testEnv.consumePGMessage('D', in),
				testEnv.consumePGMessage('D', in) };
		PgAdapterTestEnv.PGMessage commandComplete = testEnv.consumePGMessage('C', in);
		PgAdapterTestEnv.PGMessage readyForQuery = testEnv.consumePGMessage('Z', in);

		// Check RowDescription payload
		// see here: https://www.postgresql.org/docs/13/protocol-message-formats.html
		DataInputStream rowDescIn = new DataInputStream(new ByteArrayInputStream(rowDescription.getPayload()));
		short fieldCount = rowDescIn.readShort();
		assertThat(fieldCount, is(equalTo((short) 2)));
		for (String expectedFieldName : new String[] { "id", "name" }) {
			// Read a null-terminated string.
			StringBuilder builder = new StringBuilder("");
			byte b;
			while ((b = rowDescIn.readByte()) != (byte) 0) {
				builder.append((char) b);
			}
			String fieldName = builder.toString();
			assertThat(fieldName, is(equalTo(expectedFieldName)));
			byte[] unusedBytes = new byte[18];
			rowDescIn.readFully(unusedBytes);
		}

		// Check exact message results.
		byte[] rowDescriptionData = {0, 2, 105, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 19, -1, -1, 0, 0, 0, 0, 0, 0, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 19, -1, -1, 0, 0, 0, 0, 0, 0 };
		byte[] dataRow0 = {0, 2, 0, 0, 0, 1, 54, 0, 0, 0, 13, 87, 105, 110, 116, 101, 114, 32, 87, 97, 114, 109, 101, 114};
		byte[] dataRow1 = {0, 2, 0, 0, 0, 1, 56, 0, 0, 0, 13, 79, 97, 116, 109, 101, 97, 108, 32, 83, 116, 111, 117, 116};
		byte[] dataRow2 = {0, 2, 0, 0, 0, 2, 49, 48, 0, 0, 0, 15, 67, 104, 111, 99, 111, 108, 97, 116, 101, 32, 83, 116, 111, 117, 116};
		byte[] commandCompleteData = {83, 69, 76, 69, 67, 84, 32, 51, 0};
		byte[] readyForQueryData = { 73 };

		assertThat(rowDescription.getPayload(), is(equalTo(rowDescriptionData)));
		assertThat(dataRows[0].getPayload(), is(equalTo(dataRow0)));
		assertThat(dataRows[1].getPayload(), is(equalTo(dataRow1)));
		assertThat(dataRows[2].getPayload(), is(equalTo(dataRow2)));
		assertThat(commandComplete.getPayload(), is(equalTo(commandCompleteData)));
		assertThat(readyForQuery.getPayload(), is(equalTo(readyForQueryData)));
	}
}
