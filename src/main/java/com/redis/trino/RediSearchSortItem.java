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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A column a pushed-down ORDER BY sorts on, including direction and SQL null ordering.
 */
public class RediSearchSortItem {

	private final String column;
	private final boolean ascending;
	private final boolean local;
	private final boolean nullsFirst;

	public RediSearchSortItem(String column, boolean ascending) { this(column, ascending, false, false); }

	public RediSearchSortItem(String column, boolean ascending, boolean local) { this(column, ascending, local, false); }

	@JsonCreator
	public RediSearchSortItem(@JsonProperty("column") String column, @JsonProperty("ascending") boolean ascending,
			@JsonProperty("local") boolean local, @JsonProperty("nullsFirst") boolean nullsFirst) {
		this.column = requireNonNull(column, "column is null");
		this.ascending = ascending;
		this.local = local;
		this.nullsFirst = nullsFirst;
	}

	@JsonProperty
	public String getColumn() {
		return column;
	}

	@JsonProperty
	public boolean isAscending() {
		return ascending;
	}

	@JsonProperty
	public boolean isLocal() { return local; }

	@JsonProperty
	public boolean isNullsFirst() { return nullsFirst; }

	io.trino.spi.connector.SortOrder sortOrder() {
		return ascending ? (nullsFirst ? io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST : io.trino.spi.connector.SortOrder.ASC_NULLS_LAST)
				: (nullsFirst ? io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST : io.trino.spi.connector.SortOrder.DESC_NULLS_LAST);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		RediSearchSortItem other = (RediSearchSortItem) obj;
		return column.equals(other.column) && ascending == other.ascending && local == other.local && nullsFirst == other.nullsFirst;
	}

	@Override
	public int hashCode() {
		return Objects.hash(column, ascending, local, nullsFirst);
	}

	@Override
	public String toString() {
		return column + (ascending ? " ASC" : " DESC") + (nullsFirst ? " NULLS FIRST" : " NULLS LAST");
	}
}
