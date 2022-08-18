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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class RediSearchTableHandle implements ConnectorTableHandle {

	public enum Type {
		SEARCH, AGGREGATE
	}

	private final Type type;
	private final SchemaTableName schemaTableName;
	private final TupleDomain<ColumnHandle> constraint;
	private final OptionalLong limit;
	// for group by fields
	private final List<TermAggregation> termAggregations;
	private final List<MetricAggregation> metricAggregations;

	public RediSearchTableHandle(Type type, SchemaTableName schemaTableName) {
		this(type, schemaTableName, TupleDomain.all(), OptionalLong.empty(), Collections.emptyList(),
				Collections.emptyList());
	}

	@JsonCreator
	public RediSearchTableHandle(@JsonProperty("type") Type type,
			@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
			@JsonProperty("constraint") TupleDomain<ColumnHandle> constraint, @JsonProperty("limit") OptionalLong limit,
			@JsonProperty("aggTerms") List<TermAggregation> termAggregations,
			@JsonProperty("aggregates") List<MetricAggregation> metricAggregations) {
		this.type = requireNonNull(type, "type is null");
		this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
		this.constraint = requireNonNull(constraint, "constraint is null");
		this.limit = requireNonNull(limit, "limit is null");
		this.termAggregations = requireNonNull(termAggregations, "aggTerms is null");
		this.metricAggregations = requireNonNull(metricAggregations, "aggregates is null");
	}

	@JsonProperty
	public Type getType() {
		return type;
	}

	@JsonProperty
	public SchemaTableName getSchemaTableName() {
		return schemaTableName;
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
	public List<TermAggregation> getTermAggregations() {
		return termAggregations;
	}

	@JsonProperty
	public List<MetricAggregation> getMetricAggregations() {
		return metricAggregations;
	}

	@Override
	public int hashCode() {
		return Objects.hash(schemaTableName, constraint, limit);
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
		return Objects.equals(this.schemaTableName, other.schemaTableName)
				&& Objects.equals(this.constraint, other.constraint) && Objects.equals(this.limit, other.limit);
	}

	@Override
	public String toString() {
		return schemaTableName.toString();
	}
}
