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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class RediSearchTableHandle implements ConnectorTableHandle {

	private final SchemaTableName schemaTableName;
	private final String index;
	private final TupleDomain<ColumnHandle> constraint;
	private final OptionalLong limit;
	// for group by fields
	private final List<RediSearchAggregationTerm> aggregationTerms;
	private final List<RediSearchAggregation> aggregations;
	// A pushed-down ORDER BY, which keeps the first limit documents
	private final List<RediSearchSortItem> sort;

	public RediSearchTableHandle(SchemaTableName schemaTableName, String index) {
		this(schemaTableName, index, TupleDomain.all(), OptionalLong.empty(), Collections.emptyList(),
				Collections.emptyList(), Collections.emptyList());
	}

	@JsonCreator
	public RediSearchTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
			@JsonProperty("index") String index, @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
			@JsonProperty("limit") OptionalLong limit,
			@JsonProperty("aggTerms") List<RediSearchAggregationTerm> termAggregations,
			@JsonProperty("aggregates") List<RediSearchAggregation> metricAggregations,
			@JsonProperty("sort") List<RediSearchSortItem> sort) {
		this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
		this.index = requireNonNull(index, "index is null");
		this.constraint = requireNonNull(constraint, "constraint is null");
		this.limit = requireNonNull(limit, "limit is null");
		this.aggregationTerms = requireNonNull(termAggregations, "aggTerms is null");
		this.aggregations = requireNonNull(metricAggregations, "aggregates is null");
		this.sort = requireNonNull(sort, "sort is null");
	}

	public RediSearchTableHandle withConstraint(TupleDomain<ColumnHandle> constraint) {
		return new RediSearchTableHandle(schemaTableName, index, constraint, limit, aggregationTerms, aggregations, sort);
	}

	public RediSearchTableHandle withLimit(long limit) {
		return new RediSearchTableHandle(schemaTableName, index, constraint, OptionalLong.of(limit), aggregationTerms,
				aggregations, sort);
	}

	public RediSearchTableHandle withAggregations(List<RediSearchAggregationTerm> terms,
			List<RediSearchAggregation> metrics) {
		return new RediSearchTableHandle(schemaTableName, index, constraint, limit, terms, metrics, sort);
	}

	public RediSearchTableHandle withTopN(List<RediSearchSortItem> sort, long limit) {
		return new RediSearchTableHandle(schemaTableName, index, constraint, OptionalLong.of(limit), aggregationTerms,
				aggregations, sort);
	}

	@JsonProperty
	public SchemaTableName getSchemaTableName() {
		return schemaTableName;
	}

	@JsonProperty
	public String getIndex() {
		return index;
	}

	@JsonProperty
	public TupleDomain<ColumnHandle> getConstraint() {
		return constraint;
	}

	@JsonProperty
	public OptionalLong getLimit() {
		return limit;
	}

	@JsonProperty
	public List<RediSearchAggregationTerm> getTermAggregations() {
		return aggregationTerms;
	}

	@JsonProperty
	public List<RediSearchAggregation> getMetricAggregations() {
		return aggregations;
	}

	@JsonProperty
	public List<RediSearchSortItem> getSort() {
		return sort;
	}

	@Override
	public int hashCode() {
		return Objects.hash(schemaTableName, index, constraint, limit, aggregationTerms, aggregations, sort);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		RediSearchTableHandle other = (RediSearchTableHandle) obj;
		return Objects.equals(this.schemaTableName, other.schemaTableName) && Objects.equals(this.index, other.index)
				&& Objects.equals(this.constraint, other.constraint) && Objects.equals(this.limit, other.limit)
				&& Objects.equals(this.aggregationTerms, other.aggregationTerms)
				&& Objects.equals(this.aggregations, other.aggregations) && Objects.equals(this.sort, other.sort);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("schemaTableName", schemaTableName).add("index", index)
				.add("limit", limit).add("sort", sort).add("constraint", constraint).toString();
	}
}
