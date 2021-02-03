/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redislabs.redisearch.jdbc;

import com.redislabs.lettusearch.search.Limit;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.test.CalciteAssert;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.search.Document;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@Testcontainers
public abstract class AbstractBaseTest {

  public final static String ABV = "abv";
  public final static String ID = "id";
  public final static String NAME = "name";
  public final static String STYLE = "style";
  public final static String BREWERY_ID = "brewery_id";
  public final static Schema<String> SCHEMA =
      Schema.of(Field.text(NAME).matcher(PhoneticMatcher.English).build(),
          Field.tag(STYLE).sortable(true).build(), Field.numeric(ABV).sortable(true).build(),
          Field.tag(BREWERY_ID).sortable(true).build());
  public final static String BEERS = "beers";
  public final static int DEFAULT_LIMIT_NUM = Math.toIntExact(Limit.DEFAULT_NUM);

  protected static RediSearchClient client;
  protected static StatefulRediSearchConnection<String, String> rediSearchConnection;
  protected static String host;
  protected static int port;

  @Container
  @SuppressWarnings("rawtypes")
  public static final GenericContainer REDISEARCH = new GenericContainer(DockerImageName.parse(
      "redislabs/redisearch:latest")).withExposedPorts(6379);

  @BeforeAll
  public static void setup() throws IOException {
    host = REDISEARCH.getHost();
    port = REDISEARCH.getFirstMappedPort();
    client = RediSearchClient.create(RedisURI.create(host, port));
    rediSearchConnection = client.connect();
    RediSearchCommands<String, String> sync = rediSearchConnection.sync();
    sync.flushall();
    List<Document<String, String>> beers = beers();
    sync.create(BEERS, SCHEMA);
    RediSearchAsyncCommands<String, String> async = rediSearchConnection.async();
    async.setAutoFlushCommands(false);
    List<RedisFuture<?>> futures = new ArrayList<>();
    for (Document<String, String> beer : beers) {
      futures.add(async.add(BEERS, beer));
    }
    async.flushCommands();
    async.setAutoFlushCommands(true);
    LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
  }

  @AfterAll
  protected static void teardown() {
    if (rediSearchConnection != null) {
      rediSearchConnection.close();
    }
    if (client != null) {
      client.shutdown();
    }
  }

  protected static List<Document<String, String>> beers() throws IOException {
    List<Document<String, String>> beers = new ArrayList<>();
    CsvSchema schema = CsvSchema.builder().setUseHeader(true).setNullValue("").build();
    CsvMapper mapper = new CsvMapper();
    InputStream inputStream =
        AbstractBaseTest.class.getClassLoader().getResourceAsStream("beers" + ".csv");
    MappingIterator<Document<String, String>> iterator =
        mapper.readerFor(Document.class).with(schema).readValues(inputStream);
    iterator.forEachRemaining(b -> {
      if (b.get(ABV) != null) {
        b.setId(b.get(ID));
        b.setScore(1d);
        b.setPayload(b.get(NAME));
        beers.add(b);
      }
    });
    return beers;
  }

  protected CalciteAssert.ConnectionFactory newConnectionFactory() {
    return new CalciteAssert.ConnectionFactory() {
      @Override
      public Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("redisearch", new RediSearchSchema(rediSearchConnection, BEERS));

        // add calcite view programmatically
        final String viewSql = "select cast(_MAP['name'] AS varchar(200)) AS \"name\", " + " cast"
            + "(_MAP['abv']" + " AS float) AS \"abv\",\n" + " cast(_MAP['ounces']" + " AS " +
            "float) AS \"ounces\",\n" + " cast(_MAP['ibu'] AS integer) AS \"ibu\", " + " cast" +
            "(_MAP['style'] AS varchar(200)) AS \"style\", " + " cast" + "(_MAP['brewery_id'] AS " +
            "integer) AS \"brewery_id\", " + " cast(_MAP['id'] AS " + "varchar" + "(5)) AS \"id\"" +
            " " + "from \"redisearch\".\"beers\"";

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql, Collections.singletonList(
            "redisearch"), Arrays.asList("redisearch", "view"), false);
        root.add("beers", macro);

        return connection;
      }
    };
  }

  protected CalciteAssert.AssertThat calciteAssert() {
    return CalciteAssert.that().with(newConnectionFactory());
  }

}
