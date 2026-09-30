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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RediSearchMergeTableHandle implements ConnectorMergeTableHandle {
	private final RediSearchTableHandle tableHandle;
	private final List<RediSearchColumnHandle> dataColumns;
	// UPDATE case number -> channels (indexes into dataColumns) of the columns it assigns
	private final Map<Integer, List<Integer>> updateCaseChannels;

	@JsonCreator
	public RediSearchMergeTableHandle(@JsonProperty("tableHandle") RediSearchTableHandle tableHandle,
			@JsonProperty("dataColumns") List<RediSearchColumnHandle> dataColumns,
			@JsonProperty("updateCaseChannels") Map<Integer, List<Integer>> updateCaseChannels) {
		this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
		this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
		this.updateCaseChannels = ImmutableMap.copyOf(requireNonNull(updateCaseChannels, "updateCaseChannels is null"));
	}

	@Override
	@JsonProperty
	public RediSearchTableHandle getTableHandle() {
		return tableHandle;
	}

	@JsonProperty
	public List<RediSearchColumnHandle> getDataColumns() {
		return dataColumns;
	}

	@JsonProperty
	public Map<Integer, List<Integer>> getUpdateCaseChannels() {
		return updateCaseChannels;
	}
}
