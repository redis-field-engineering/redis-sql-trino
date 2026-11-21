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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.trino.spi.TrinoException;

/** Maps SQL aliases to unambiguous scalar JSON object paths. Array/wildcard writes are deliberately rejected. */
final class RediSearchJsonWriter {

	private static final Pattern OBJECT_PATH = Pattern.compile("\\$((\\.[A-Za-z_][A-Za-z_0-9]*)+)");
	private static final JsonFactory JSON = new JsonFactory();
	private final Map<String, List<String>> paths = new LinkedHashMap<>();
	private final Map<String, RediSearchFieldType> types = new LinkedHashMap<>();

	RediSearchJsonWriter(RediSearchIndexInfo info) {
		for (RediSearchIndexInfo.Field field : info.getFields()) {
			String path = field.getIdentifier();
			if (!OBJECT_PATH.matcher(path).matches()
					|| (field.getType() != RediSearchFieldType.TAG && field.getType() != RediSearchFieldType.TEXT
							&& field.getType() != RediSearchFieldType.NUMERIC)) {
				throw unsupported("JSON writes require scalar object paths; unsupported path/type: " + path);
			}
			List<String> components = List.of(path.substring(2).split("\\."));
			for (List<String> previous : paths.values()) {
				int common = Math.min(previous.size(), components.size());
				if (previous.subList(0, common).equals(components.subList(0, common))) {
					throw unsupported("JSON writes do not support duplicate or overlapping indexed paths: " + path);
				}
			}
			if (paths.putIfAbsent(field.getAttribute(), components) != null) {
				throw unsupported("JSON writes do not support duplicate aliases: " + field.getAttribute());
			}
			types.put(field.getAttribute(), field.getType());
		}
	}

	/** Null values are represented as JSON null for RFC 7396 merge-patch deletion. Inserts omit null columns. */
	String document(Map<String, String> values) {
		Map<String, Object> document = new LinkedHashMap<>();
		for (Map.Entry<String, String> entry : values.entrySet()) {
			List<String> path = paths.get(entry.getKey());
			if (path == null) {
				throw unsupported("JSON writes require an indexed scalar path for column: " + entry.getKey());
			}
			Map<String, Object> object = document;
			for (String component : path.subList(0, path.size() - 1)) {
				@SuppressWarnings("unchecked")
				Map<String, Object> child = (Map<String, Object>) object.computeIfAbsent(component, ignored -> new LinkedHashMap<>());
				object = child;
			}
			Object value = entry.getValue();
			if (value != null && types.get(entry.getKey()) == RediSearchFieldType.NUMERIC) {
				try {
					value = new BigDecimal(entry.getValue());
				} catch (NumberFormatException e) {
					throw unsupported("JSON numeric writes require a finite number: " + entry.getKey());
				}
			}
			object.put(path.getLast(), value);
		}
		try {
			StringWriter output = new StringWriter();
			try (JsonGenerator generator = JSON.createGenerator(output)) {
				writeObject(generator, document);
			}
			return output.toString();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static void writeObject(JsonGenerator generator, Map<String, Object> object) throws IOException {
		generator.writeStartObject();
		for (Map.Entry<String, Object> entry : object.entrySet()) {
			generator.writeFieldName(entry.getKey());
			if (entry.getValue() == null) {
				generator.writeNull();
			} else if (entry.getValue() instanceof Map<?, ?> nested) {
				@SuppressWarnings("unchecked")
				Map<String, Object> child = (Map<String, Object>) nested;
				writeObject(generator, child);
			} else if (entry.getValue() instanceof BigDecimal number) {
				generator.writeNumber(number);
			} else {
				generator.writeString((String) entry.getValue());
			}
		}
		generator.writeEndObject();
	}

	private static TrinoException unsupported(String message) {
		return new TrinoException(NOT_SUPPORTED, message);
	}
}
