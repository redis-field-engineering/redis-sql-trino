package com.redis.trino;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RediSearchTable {

	private final RediSearchTableHandle tableHandle;
	private final List<RediSearchColumnHandle> columns;

	public RediSearchTable(RediSearchTableHandle tableHandle, List<RediSearchColumnHandle> columns) {
		this.tableHandle = tableHandle;
		this.columns = ImmutableList.copyOf(columns);
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
