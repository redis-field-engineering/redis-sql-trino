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

import org.apache.calcite.runtime.Hook;

import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateResults;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;

import java.util.Objects;
import java.util.function.Function;

final class RediSearchTransport {

  static final int DEFAULT_FETCH_SIZE = 5196;

  private final StatefulRediSearchConnection<String, String> connection;

  final String indexName;

  /**
   * Default batch size.
   */
  final int fetchSize;
  private final IndexInfo<String> indexInfo;

  RediSearchTransport(final StatefulRediSearchConnection<String, String> connection,
      final String indexName, final int fetchSize) {
    this.connection = Objects.requireNonNull(connection, "restClient");
    this.indexName = Objects.requireNonNull(indexName, "indexName");
    this.fetchSize = fetchSize;
    this.indexInfo = RediSearchUtils.getInfo(connection.sync().ftInfo(indexName));
  }

  public IndexInfo<String> getIndexInfo() {
    return indexInfo;
  }

  StatefulRediSearchConnection<String, String> connection() {
    return this.connection;
  }

  /**
   * Fetches search results given a scrollId.
   */
  Function<String, AggregateWithCursorResults<String, String>> scroll() {
    return scrollId -> connection.sync().cursorRead(indexName, Long.parseLong(scrollId));
  }

  void closeScroll(Iterable<String> scrollIds) {
    Objects.requireNonNull(scrollIds, "scrollIds");
    for (String scrollId : scrollIds) {
      connection.sync().cursorDelete(indexName, Long.parseLong(scrollId));
    }
  }

  /**
   * Search request using HTTP post.
   */
  SearchResults<String, String> search(String query, SearchOptions<String> options) {
    Hook.QUERY_PLAN.run(query);
    return connection.sync().search(indexName, query, options);
  }

  AggregateResults<String, String> aggregate(String query, AggregateOptions options) {
    Hook.QUERY_PLAN.run(query);
    return connection.sync().aggregate(indexName, query, options);
  }

}
