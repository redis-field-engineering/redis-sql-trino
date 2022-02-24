package com.redis.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RediSearchInsertTableHandle implements ConnectorInsertTableHandle {
	private final SchemaTableName schemaTableName;
	private final List<RediSearchColumnHandle> columns;

	@JsonCreator
	public RediSearchInsertTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
			@JsonProperty("columns") List<RediSearchColumnHandle> columns) {
		this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
		this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
	}

	@JsonProperty
	public SchemaTableName getSchemaTableName() {
		return schemaTableName;
	}

	@JsonProperty
	public List<RediSearchColumnHandle> getColumns() {
		return columns;
	}
}
