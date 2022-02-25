package com.redis.trino;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.OptionalInt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

public class RediSearchTableHandle implements ConnectorTableHandle {

	private final SchemaTableName schemaTableName;
	private final TupleDomain<ColumnHandle> constraint;
	private final OptionalInt limit;

	public RediSearchTableHandle(SchemaTableName schemaTableName) {
		this(schemaTableName, TupleDomain.all(), OptionalInt.empty());
	}

	@JsonCreator
	public RediSearchTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
			@JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
			@JsonProperty("limit") OptionalInt limit) {
		this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
		this.constraint = requireNonNull(constraint, "constraint is null");
		this.limit = requireNonNull(limit, "limit is null");
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
	public OptionalInt getLimit() {
		return limit;
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
