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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.lettuce.core.search.FieldValue;

/**
 * Reads the values of an FT.AGGREGATE row by column name, from the fields the scan loaded them as.
 */
public class RediSearchRowReader {

	/**
	 * Reads each column from the field of its name, as returned.
	 */
	public static final RediSearchRowReader BY_NAME = new RediSearchRowReader(Map.of(), Set.of());

	private static final JsonFactory JSON = new JsonFactory();

	private final Map<String, String> sources;
	private final Set<String> jsonArrays;

	/**
	 * @param sources    the field each column is read from, for columns not read from the field of their name
	 * @param jsonArrays columns whose values come as JSON arrays of the values at their JSON path, as DIALECT 3
	 *                   returns them
	 */
	public RediSearchRowReader(Map<String, String> sources, Set<String> jsonArrays) {
		this.sources = ImmutableMap.copyOf(requireNonNull(sources, "sources is null"));
		this.jsonArrays = ImmutableSet.copyOf(requireNonNull(jsonArrays, "jsonArrays is null"));
	}

	public Map<String, String> read(Map<String, FieldValue> fields) {
		Map<String, String> row = new HashMap<>();
		for (Map.Entry<String, FieldValue> field : fields.entrySet()) {
			FieldValue value = field.getValue();
			if (value != null && !value.isNull()) {
				row.put(field.getKey(), value.asString());
			}
		}
		sources.forEach((column, source) -> put(row, column, Optional.ofNullable(row.get(source))));
		for (String column : jsonArrays) {
			String array = row.get(column);
			if (array != null) {
				put(row, column, firstValue(array));
			}
		}
		return row;
	}

	private static void put(Map<String, String> row, String column, Optional<String> value) {
		if (value.isPresent()) {
			row.put(column, value.get());
		} else {
			row.remove(column);
		}
	}

	/**
	 * The first value in a JSON array, as DIALECT 2 returns it: a string as its value, a number as written, so as many
	 * digits as were stored, a boolean as {@code 1} or {@code 0}, and an object or array as its JSON text. Empty for
	 * {@code null} or an empty array.
	 */
	static Optional<String> firstValue(String array) {
		try (JsonParser parser = JSON.createParser(array)) {
			if (parser.nextToken() != JsonToken.START_ARRAY) {
				throw new IllegalArgumentException("Not a JSON array: " + array);
			}
			JsonToken token = parser.nextToken();
			switch (token) {
			case END_ARRAY:
			case VALUE_NULL:
				return Optional.empty();
			case VALUE_TRUE:
				return Optional.of("1");
			case VALUE_FALSE:
				return Optional.of("0");
			case START_OBJECT:
			case START_ARRAY:
				int start = Math.toIntExact(parser.currentTokenLocation().getCharOffset());
				parser.skipChildren();
				int end = Math.toIntExact(parser.currentLocation().getCharOffset());
				return Optional.of(array.substring(start, end));
			default:
				return Optional.of(parser.getText());
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Invalid JSON array: " + array, e);
		}
	}
}
