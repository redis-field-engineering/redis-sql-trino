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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.FieldType;
import com.redislabs.lettusearch.search.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Table based on an Elasticsearch index.
 */
public class RediSearchTable extends AbstractQueryableTable implements TranslatableTable {

  private final String indexName;
  final RediSearchTransport transport;

  /**
   * Creates an ElasticsearchTable.
   */
  RediSearchTable(RediSearchTransport transport) {
    super(Object[].class);
    this.transport = Objects.requireNonNull(transport, "transport");
    this.indexName = transport.indexName;
  }


  /**
   * Executes a "find" operation on the underlying index.
   *
   * @param ops          List of operations.
   * @param fields       List of fields to project; or null to return map
   * @param sort         list of fields to sort and their direction (asc/desc)
   * @param aggregations aggregation functions
   * @return Enumerator of results
   */
  private Enumerable<Document<String, String>> find(List<String> ops, List<Map.Entry<String,
      Class>> fields, List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      List<String> groupBy, List<Map.Entry<String, String>> aggregations,
      Map<String, String> mappings, Long offset, Long fetch) throws IOException {

    if (!aggregations.isEmpty() || !groupBy.isEmpty()) {
      // process aggregations separately
      throw new UnsupportedOperationException("Aggregations not yet supported");
      //      return aggregate(ops, fields, sort, groupBy, aggregations, mappings, offset, fetch);
    }

    String query = "*";
    // manually parse from previously concatenated string
    for (String op : ops) {
      query = op;
    }

    SearchOptions.SearchOptionsBuilder<String> options = SearchOptions.builder();

    if (!sort.isEmpty()) {
      sort.forEach(e -> options.sortBy(SortBy.<String>builder().field(e.getKey()).direction(e.getValue().isDescending() ? Direction.Descending : Direction.Ascending).build()));
    }

    Limit.LimitBuilder limit = Limit.builder();
    if (offset != null) {
      limit.offset(offset);
    }
    if (fetch != null) {
      limit.num(fetch);
    }
    options.limit(limit.build());

    final Function1<Document<String, String>, Document<String, String>> getter =
        RediSearchEnumerators.getter(fields, ImmutableMap.copyOf(mappings));

    SearchResults<String, String> results = transport.search(query, options.build());
    return Linq4j.asEnumerable(results).select(getter);
  }

  private Enumerable<Map<String, String>> aggregate(List<String> ops, List<Map.Entry<String,
      Class>> fields, List<Map.Entry<String, RelFieldCollation.Direction>> sort,
      List<String> groupBy, List<Map.Entry<String, String>> aggregations,
      Map<String, String> mapping, Long offset, Long fetch) throws IOException {
    throw new UnsupportedOperationException("Aggregations not yet supported");

  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    final RelDataType mapType =
        relDataTypeFactory.createMapType(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
            relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(SqlTypeName.ANY), true));
    return relDataTypeFactory.builder().add("_MAP", mapType).build();
  }


  @Override
  public String toString() {
    return "RediSearchTable{" + indexName + "}";
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new RediSearchQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new RediSearchTableScan(cluster, cluster.traitSetOf(RediSearchRel.CONVENTION),
        relOptTable, this, null);
  }

  /**
   * Implementation of {@link Queryable} based on
   * a {@link RediSearchTable}.
   *
   * @param <T> element type
   */
  public static class RediSearchQueryable<T> extends AbstractTableQueryable<T> {
    RediSearchQueryable(QueryProvider queryProvider, SchemaPlus schema, RediSearchTable table,
        String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      return null;
    }

    private RediSearchTable getTable() {
      return (RediSearchTable) table;
    }

    /**
     * Called via code-generation.
     *
     * @param ops    list of queries (as strings)
     * @param fields projection
     * @return result as enumerable
     * @see RediSearchMethod#REDISEARCH_QUERYABLE_FIND
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Document<String, String>> find(List<String> ops, List<Map.Entry<String,
        Class>> fields, List<Map.Entry<String, RelFieldCollation.Direction>> sort,
        List<String> groupBy, List<Map.Entry<String, String>> aggregations,
        Map<String, String> mappings, Long offset, Long fetch) {
      try {
        return getTable().find(ops, fields, sort, groupBy, aggregations, mappings, offset, fetch);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to query " + getTable().indexName, e);
      }
    }

  }
}
