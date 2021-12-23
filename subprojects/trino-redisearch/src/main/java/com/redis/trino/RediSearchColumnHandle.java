package com.redis.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RediSearchColumnHandle implements ColumnHandle {

	private final String name;
	private final Type type;
	private final boolean hidden;

	@JsonCreator
	public RediSearchColumnHandle(@JsonProperty("name") String name, @JsonProperty("columnType") Type type,
			@JsonProperty("hidden") boolean hidden) {
		this.name = requireNonNull(name, "name is null");
		this.type = requireNonNull(type, "type is null");
		this.hidden = hidden;
	}

	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty("columnType")
	public Type getType() {
		return type;
	}

	@JsonProperty
	public boolean isHidden() {
		return hidden;
	}

	public ColumnMetadata toColumnMetadata() {
		return ColumnMetadata.builder().setName(name).setType(type).setHidden(hidden).build();
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, type, hidden);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		RediSearchColumnHandle other = (RediSearchColumnHandle) obj;
		return Objects.equals(name, other.name) && Objects.equals(type, other.type)
				&& Objects.equals(hidden, other.hidden);
	}

	@Override
	public String toString() {
		return toStringHelper(this).add("name", name).add("type", type).add("hidden", hidden).toString();
	}
}
