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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.redislabs.lettusearch.RediSearchClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory that creates an {@link RediSearchSchema}.
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class RediSearchSchemaFactory implements SchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RediSearchSchemaFactory.class);

  public RediSearchSchemaFactory() {
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

    String uri = (String) operand.getOrDefault("uri", "redis://localhost:6379");
    final RediSearchClient client = RediSearchClient.create(uri);
    final String index = (String) operand.get("index");

    return new RediSearchSchema(client.connect(), index);
  }


}
