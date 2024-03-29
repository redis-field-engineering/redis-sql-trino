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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.redis.lettucemod.search.IndexInfo;

public class RediSearchTable {

	private final RediSearchTableHandle tableHandle;
	private final List<RediSearchColumnHandle> columns;
	private final IndexInfo indexInfo;

	public RediSearchTable(RediSearchTableHandle tableHandle, List<RediSearchColumnHandle> columns,
			IndexInfo indexInfo) {
		this.tableHandle = tableHandle;
		this.columns = ImmutableList.copyOf(columns);
		this.indexInfo = indexInfo;
	}

	public IndexInfo getIndexInfo() {
		return indexInfo;
	}

	public RediSearchTableHandle getTableHandle() {
		return tableHandle;
	}

	public List<RediSearchColumnHandle> getColumns() {
		return columns;
	}

	@Override
	public int hashCode() {
		return tableHandle.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof RediSearchTable)) {
			return false;
		}
		RediSearchTable that = (RediSearchTable) obj;
		return this.tableHandle.equals(that.tableHandle);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("tableHandle", tableHandle).toString();
	}
}
