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
