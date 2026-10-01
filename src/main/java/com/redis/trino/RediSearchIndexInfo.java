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
 * The subset of an FT.INFO reply that the connector uses: key type, key prefixes, fields and indexing progress.
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
		private final Optional<Character> separator;

		public Field(String attribute, RediSearchFieldType type, Optional<Character> separator) {
			this.attribute = requireNonNull(attribute, "attribute is null");
			this.type = requireNonNull(type, "type is null");
			this.separator = requireNonNull(separator, "separator is null");
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

		/**
		 * @return the character a TAG field's values are split into tags on; empty for other fields and for JSON TAG
		 *         fields, which don't split values by default
		 */
		public Optional<Character> getSeparator() {
			return separator;
		}
	}

	private static final char DEFAULT_TAG_SEPARATOR = ',';

	private final Optional<KeyType> keyType;
	private final List<String> prefixes;
	private final List<Field> fields;
	private final boolean indexing;
	private final double percentIndexed;
	private final boolean customStopwords;

	public RediSearchIndexInfo(Optional<KeyType> keyType, List<String> prefixes, List<Field> fields, boolean indexing,
			double percentIndexed, boolean customStopwords) {
		this.keyType = requireNonNull(keyType, "keyType is null");
		this.prefixes = ImmutableList.copyOf(requireNonNull(prefixes, "prefixes is null"));
		this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
		this.indexing = indexing;
		this.percentIndexed = percentIndexed;
		this.customStopwords = customStopwords;
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

	/**
	 * @return whether the index is still indexing existing documents in the background, in which case queries
	 *         return only the documents indexed so far
	 */
	public boolean isIndexing() {
		return indexing;
	}

	/**
	 * @return the fraction of existing documents indexed so far, from 0 to 1
	 */
	public double getPercentIndexed() {
		return percentIndexed;
	}

	/**
	 * @return whether the index was created with its own STOPWORDS list instead of the default one
	 */
	public boolean hasCustomStopwords() {
		return customStopwords;
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
				RediSearchFieldType type = RediSearchFieldType.of(string(attributeMap.get("type")));
				fields.add(new Field(alias == null ? identifier : alias, type, separator(type, attributeMap)));
			}
		}
		boolean indexing = number(info.get("indexing"), 0) != 0;
		double percentIndexed = number(info.get("percent_indexed"), 1);
		// Only listed for an index created with STOPWORDS
		boolean customStopwords = info.containsKey("stopwords_list");
		return new RediSearchIndexInfo(keyType, prefixes, fields, indexing, percentIndexed, customStopwords);
	}

	// FT.INFO lists each TAG field's SEPARATOR, empty when values aren't split; without one, assume the default
	private static Optional<Character> separator(RediSearchFieldType type, Map<String, Object> attribute) {
		if (type != RediSearchFieldType.TAG) {
			return Optional.empty();
		}
		String separator = string(attribute.get("SEPARATOR"));
		if (separator == null) {
			return Optional.of(DEFAULT_TAG_SEPARATOR);
		}
		return separator.isEmpty() ? Optional.empty() : Optional.of(separator.charAt(0));
	}

	// FT.INFO numbers arrive as integers, doubles or strings depending on the field and protocol
	private static double number(Object value, double defaultValue) {
		if (value instanceof Number) {
			return ((Number) value).doubleValue();
		}
		String string = string(value);
		if (string == null) {
			return defaultValue;
		}
		try {
			return Double.parseDouble(string);
		} catch (NumberFormatException e) {
			return defaultValue;
		}
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
