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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.redis.lettucemod.search.Field;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

public class RediSearchColumnHandle implements ColumnHandle {

	private final String name;
	private final Type type;
	private final Field.Type fieldType;
	private final boolean hidden;
	private final boolean supportsPredicates;

	@JsonCreator
	public RediSearchColumnHandle(@JsonProperty("name") String name, @JsonProperty("columnType") Type type,
			@JsonProperty("fieldType") Field.Type fieldType, @JsonProperty("hidden") boolean hidden,
			@JsonProperty("supportsPredicates") boolean supportsPredicates) {
		this.name = requireNonNull(name, "name is null");
		this.type = requireNonNull(type, "type is null");
		this.fieldType = requireNonNull(fieldType, "fieldType is null");
		this.hidden = hidden;
		this.supportsPredicates = supportsPredicates;
	}

	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty("columnType")
	public Type getType() {
		return type;
	}

	@JsonProperty("fieldType")
	public Field.Type getFieldType() {
		return fieldType;
	}

	@JsonProperty
	public boolean isHidden() {
		return hidden;
	}

	@JsonProperty
	public boolean isSupportsPredicates() {
		return supportsPredicates;
	}

	public ColumnMetadata toColumnMetadata() {
		return ColumnMetadata.builder().setName(name).setType(type).setHidden(hidden).build();
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, type, fieldType, hidden, supportsPredicates);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		RediSearchColumnHandle other = (RediSearchColumnHandle) obj;
		return Objects.equals(name, other.name) && Objects.equals(type, other.type) && this.fieldType == other.fieldType
				&& this.hidden == other.hidden && this.supportsPredicates == other.supportsPredicates;
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("name", name).add("type", type).add("fieldType", fieldType)
				.add("hidden", hidden).add("supportsPredicates", supportsPredicates).toString();
	}
}
