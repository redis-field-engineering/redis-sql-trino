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

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Objects;
import java.util.Map;

/** Single-key update with atomic visibility; script failure is not transaction rollback. */
final class RediSearchHashUpdate {
	static final String SCRIPT = """
			local n = tonumber(ARGV[1])
			if not redis.acl_check_cmd('HSET', KEYS[1], ARGV[2], ARGV[3]) then
			  return redis.error_reply('NOPERM atomic HASH update requires HSET permission')
			end
			if #ARGV > 2*n+1 and not redis.acl_check_cmd('HDEL', KEYS[1], ARGV[2*n+2]) then
			  return redis.error_reply('NOPERM atomic HASH update requires HDEL permission')
			end
			for i = 1, n do
			  redis.call('HSET', KEYS[1], ARGV[2*i], ARGV[2*i+1])
			end
			for i = 2*n+2, #ARGV do
			  redis.call('HDEL', KEYS[1], ARGV[i])
			end
			return 1
			""";

	private RediSearchHashUpdate() {}

	static Map<String, String> fieldNames(RediSearchIndexInfo info) {
		Map<String, String> names = new HashMap<>();
		info.getFields().forEach(field -> names.put(field.getAttribute(), field.getIdentifier()));
		return names;
	}

	static void putValue(Map<String, String> values, String field, String value) {
		if (values.containsKey(field) && !Objects.equals(values.get(field), value)) {
			throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.NOT_SUPPORTED,
					"Conflicting assignments to aliases of HASH field: " + field);
		}
		values.put(field, value);
	}

	static String[] arguments(Map<String, String> values, List<String> nulls) {
		if (values.isEmpty()) {
			throw new IllegalArgumentException("HASH update requires the row marker");
		}
		List<String> arguments = new ArrayList<>();
		arguments.add(Integer.toString(values.size()));
		values.forEach((field, value) -> {
			arguments.add(requireNonNull(field, "field is null"));
			arguments.add(requireNonNull(value, "value is null"));
		});
		nulls.forEach(field -> arguments.add(requireNonNull(field, "field is null")));
		return arguments.toArray(String[]::new);
	}
}
