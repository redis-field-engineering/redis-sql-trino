package com.redis.trino;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

public class TermAggregation {
	private final String term;
	private final Type type;

	@JsonCreator
	public TermAggregation(@JsonProperty("term") String term, @JsonProperty("type") Type type) {
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

	public static Optional<TermAggregation> fromColumnHandle(ColumnHandle columnHandle) {
		RediSearchColumnHandle column = (RediSearchColumnHandle) columnHandle;
		return Optional.of(new TermAggregation(column.getName(), column.getType()));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TermAggregation that = (TermAggregation) o;
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
