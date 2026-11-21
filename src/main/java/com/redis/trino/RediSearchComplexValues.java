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

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

/** Versioned nested-value encoding. Scalars are strings, preserving every supported scalar's lossless encoding. */
final class RediSearchComplexValues {
	private static final String PREFIX = "trino:v1:";
	private static final JsonFactory JSON = new JsonFactory();

	private RediSearchComplexValues() {}

	static boolean isComplex(Type type) {
		return type instanceof ArrayType || type instanceof MapType || type instanceof RowType;
	}

	static String encode(Type type, Block block, int position) {
		try {
			StringWriter output = new StringWriter();
			try (JsonGenerator generator = JSON.createGenerator(output)) {
				write(type, block, position, generator);
			}
			return PREFIX + output;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static void write(Type type, Block block, int position, JsonGenerator out) throws IOException {
		if (block.isNull(position)) {
			out.writeNull();
		} else if (type instanceof ArrayType array) {
			Block values = array.getObject(block, position);
			out.writeStartArray();
			for (int i = 0; i < values.getPositionCount(); i++) {
				write(array.getElementType(), values, i, out);
			}
			out.writeEndArray();
		} else if (type instanceof MapType map) {
			SqlMap values = map.getObject(block, position);
			out.writeStartArray();
			for (int i = 0; i < values.getSize(); i++) {
				out.writeStartArray();
				write(map.getKeyType(), values.getRawKeyBlock(), values.getRawOffset() + i, out);
				write(map.getValueType(), values.getRawValueBlock(), values.getRawOffset() + i, out);
				out.writeEndArray();
			}
			out.writeEndArray();
		} else if (type instanceof RowType row) {
			SqlRow values = row.getObject(block, position);
			out.writeStartArray();
			for (int i = 0; i < row.getFieldTypes().size(); i++) {
				write(row.getFieldTypes().get(i), values.getRawFieldBlock(i), values.getRawIndex(), out);
			}
			out.writeEndArray();
		} else {
			out.writeString(RediSearchPageSink.value(type, block, position));
		}
	}

	static void decode(Type type, BlockBuilder output, String value) {
		if (!value.startsWith(PREFIX)) {
			throw invalid(type);
		}
		try (JsonParser parser = JSON.createParser(value.substring(PREFIX.length()))) {
			parser.nextToken();
			// A column-level SQL NULL is an absent hash field. Only nested values may encode null.
			if (parser.currentToken() == JsonToken.VALUE_NULL) {
				throw invalid(type);
			}
			read(type, output, parser);
			if (parser.nextToken() != null) {
				throw invalid(type);
			}
		} catch (IOException | IllegalArgumentException e) {
			throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid encoded value for " + type, e);
		}
	}

	private static void read(Type type, BlockBuilder output, JsonParser parser) throws IOException {
		if (parser.currentToken() == JsonToken.VALUE_NULL) {
			output.appendNull();
		} else if (type instanceof ArrayType array) {
			require(parser, JsonToken.START_ARRAY, type);
			((ArrayBlockBuilder) output).buildEntry(elements -> {
				while (parser.nextToken() != JsonToken.END_ARRAY) {
					read(array.getElementType(), elements, parser);
				}
			});
		} else if (type instanceof MapType map) {
			require(parser, JsonToken.START_ARRAY, type);
			((MapBlockBuilder) output).strict().buildEntry((keys, values) -> {
				while (parser.nextToken() != JsonToken.END_ARRAY) {
					require(parser, JsonToken.START_ARRAY, type);
					parser.nextToken();
					if (parser.currentToken() == JsonToken.VALUE_NULL) {
						throw invalid(type);
					}
					read(map.getKeyType(), keys, parser);
					parser.nextToken();
					read(map.getValueType(), values, parser);
					parser.nextToken();
					require(parser, JsonToken.END_ARRAY, type);
				}
			});
		} else if (type instanceof RowType row) {
			require(parser, JsonToken.START_ARRAY, type);
			((RowBlockBuilder) output).buildEntry(fields -> {
				for (int i = 0; i < fields.size(); i++) {
					parser.nextToken();
					read(row.getFieldTypes().get(i), fields.get(i), parser);
				}
				parser.nextToken();
				require(parser, JsonToken.END_ARRAY, type);
			});
		} else {
			require(parser, JsonToken.VALUE_STRING, type);
			RediSearchPageSourceResultWriter.writer(type).write(output, parser.getText());
		}
	}

	private static void require(JsonParser parser, JsonToken token, Type type) {
		if (parser.currentToken() != token) {
			throw invalid(type);
		}
	}

	private static TrinoException invalid(Type type) {
		return new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid encoded value for " + type);
	}
}
