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

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

public class RediSearchAggregationTerm {

	private final String term;
	private final Type type;

	@JsonCreator
	public RediSearchAggregationTerm(@JsonProperty("term") String term, @JsonProperty("type") Type type) {
		this.term = term;
		this.type = type;
	}

	@JsonProperty
	public String getTerm() {
		return term;
	}

	@JsonProperty
	public Type getType() {
		return type;
	}

	public static Optional<RediSearchAggregationTerm> fromColumnHandle(ColumnHandle columnHandle) {
		RediSearchColumnHandle column = (RediSearchColumnHandle) columnHandle;
		return Optional.of(new RediSearchAggregationTerm(column.getName(), column.getType()));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RediSearchAggregationTerm that = (RediSearchAggregationTerm) o;
		return Objects.equals(term, that.term) && Objects.equals(type, that.type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(term, type);
	}

	@Override
	public String toString() {
		return term;
	}
}
