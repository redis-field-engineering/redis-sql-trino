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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import io.lettuce.core.codec.StringCodec;

/**
 * The subset of an FT.INFO reply that the connector uses: key type, key prefixes and fields.
 * <p>
 * Parses the reply as returned by {@link io.lettuce.core.output.NestedMultiOutput}, where RESP2 arrays and
 * RESP3 maps both arrive as flat lists of alternating keys and values.
 */
public class RediSearchIndexInfo {

	public enum KeyType {
		HASH, JSON
	}

	public static class Field {
		private final String attribute;
		private final RediSearchFieldType type;

		public Field(String attribute, RediSearchFieldType type) {
			this.attribute = requireNonNull(attribute, "attribute is null");
			this.type = requireNonNull(type, "type is null");
		}

		/**
		 * @return the field name used in queries (the AS alias, or the identifier if none)
		 */
		public String getAttribute() {
			return attribute;
		}

		public RediSearchFieldType getType() {
			return type;
		}
	}

	private final Optional<KeyType> keyType;
	private final List<String> prefixes;
	private final List<Field> fields;

	public RediSearchIndexInfo(Optional<KeyType> keyType, List<String> prefixes, List<Field> fields) {
		this.keyType = requireNonNull(keyType, "keyType is null");
		this.prefixes = ImmutableList.copyOf(requireNonNull(prefixes, "prefixes is null"));
		this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
	}

	public Optional<KeyType> getKeyType() {
		return keyType;
	}

	public List<String> getPrefixes() {
		return prefixes;
	}

	public List<Field> getFields() {
		return fields;
	}

	public static RediSearchIndexInfo parse(List<Object> reply) {
		Map<String, Object> info = toMap(reply);
		Optional<KeyType> keyType = Optional.empty();
		List<String> prefixes = new ArrayList<>();
		Object definition = info.get("index_definition");
		if (definition instanceof List) {
			Map<String, Object> definitionMap = toMap((List<?>) definition);
			keyType = Optional.ofNullable(string(definitionMap.get("key_type"))).map(KeyType::valueOf);
			Object prefixList = definitionMap.get("prefixes");
			if (prefixList instanceof List) {
				((List<?>) prefixList).stream().map(RediSearchIndexInfo::string).forEach(prefixes::add);
			}
		}
		List<Field> fields = new ArrayList<>();
		Object attributes = info.get("attributes");
		if (attributes instanceof List) {
			for (Object attribute : (List<?>) attributes) {
				Map<String, Object> attributeMap = toMap((List<?>) attribute);
				String identifier = string(attributeMap.get("identifier"));
				String alias = string(attributeMap.get("attribute"));
				fields.add(new Field(alias == null ? identifier : alias,
						RediSearchFieldType.of(string(attributeMap.get("type")))));
			}
		}
		return new RediSearchIndexInfo(keyType, prefixes, fields);
	}

	// Reads leading key/value pairs; stops at the first non-string key (e.g. trailing flags like SORTABLE).
	private static Map<String, Object> toMap(List<?> list) {
		Map<String, Object> map = new HashMap<>();
		for (int i = 0; i + 1 < list.size(); i += 2) {
			String key = string(list.get(i));
			if (key == null) {
				break;
			}
			map.putIfAbsent(key, list.get(i + 1));
		}
		return map;
	}

	private static String string(Object value) {
		if (value instanceof String) {
			return (String) value;
		}
		if (value instanceof ByteBuffer) {
			return StringCodec.UTF8.decodeKey((ByteBuffer) value);
		}
		return null;
	}
}
