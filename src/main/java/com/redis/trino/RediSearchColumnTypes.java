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

import java.util.LinkedHashMap;
import java.util.Map;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.trino.spi.type.Type;

/**
 * The Trino types of the columns the connector creates. Redis keeps only each field's index type, which columns of
 * several types share (BIGINT and DOUBLE are both NUMERIC, VARCHAR and DATE both TAG), so CREATE TABLE and ADD COLUMN
 * save the declared types as JSON in the string key {@code __trino:columns:<index>}, e.g.
 * {@code {"regionkey":"bigint","name":"varchar(25)"}}. Unlike a hash, a string is never indexed, so the key can't
 * show up as a row of an index without a key prefix.
 */
public final class RediSearchColumnTypes {

	private static final Logger log = Logger.get(RediSearchColumnTypes.class);

	private static final String KEY_PREFIX = "__trino:columns:";

	private static final JsonCodec<Map<String, String>> CODEC = JsonCodec.mapJsonCodec(String.class, String.class);

	private RediSearchColumnTypes() {
	}

	public static String key(String index) {
		return KEY_PREFIX + index;
	}

	/**
	 * @return the type ID saved for each column, or none if the types weren't saved or can't be read
	 */
	public static Map<String, String> read(RedisClusterCommands<String, String> redis, String index) {
		try {
			String json = redis.get(key(index));
			return json == null ? Map.of() : CODEC.fromJson(json);
		} catch (RedisCommandExecutionException | IllegalArgumentException e) {
			// An error reply, such as NOPERM or WRONGTYPE, or a value that isn't a JSON object of strings. Connection
			// errors fail the query instead, so wrong types aren't cached for the table.
			log.warn(e, "Could not read the column types of index %s from %s", index, key(index));
			return Map.of();
		}
	}

	/**
	 * Saves the types of an index's columns, replacing any saved before.
	 */
	public static void write(RedisClusterCommands<String, String> redis, String index, Map<String, Type> types) {
		Map<String, String> typeIds = new LinkedHashMap<>();
		types.forEach((column, type) -> typeIds.put(column, type.getTypeId().getId()));
		redis.set(key(index), CODEC.toJson(typeIds));
	}

	/**
	 * Adds the type of a column to those saved for an index.
	 */
	public static void add(RedisClusterCommands<String, String> redis, String index, String column, Type type) {
		Map<String, String> typeIds = new LinkedHashMap<>(read(redis, index));
		typeIds.put(column, type.getTypeId().getId());
		redis.set(key(index), CODEC.toJson(typeIds));
	}

	public static void delete(RedisClusterCommands<String, String> redis, String index) {
		redis.del(key(index));
	}
}
