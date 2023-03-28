package com.redis.trino;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;

import java.util.Map;
import java.util.Optional;

import com.redis.lettucemod.search.Field;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

enum RediSearchBuiltinField {

	KEY("__key", VARCHAR, Field.Type.TAG);

	private static final Map<String, RediSearchBuiltinField> COLUMNS_BY_NAME = stream(values())
			.collect(toImmutableMap(RediSearchBuiltinField::getName, identity()));

	private final String name;
	private final Type type;
	private final Field.Type fieldType;

	RediSearchBuiltinField(String name, Type type, Field.Type fieldType) {
		this.name = name;
		this.type = type;
		this.fieldType = fieldType;
	}

	public static Optional<RediSearchBuiltinField> of(String name) {
		return Optional.ofNullable(COLUMNS_BY_NAME.get(name));
	}

	public static boolean isBuiltinColumn(String name) {
		return COLUMNS_BY_NAME.containsKey(name);
	}

	public String getName() {
		return name;
	}

	public Type getType() {
		return type;
	}

	public Field.Type getFieldType() {
		return fieldType;
	}

	public ColumnMetadata getMetadata() {
		return ColumnMetadata.builder().setName(name).setType(type).setHidden(true).build();
	}

	public RediSearchColumnHandle getColumnHandle() {
		return new RediSearchColumnHandle(name, type, fieldType, true, false);
	}

	public static boolean isKeyColumn(String columnName) {
		return KEY.name.equals(columnName);
	}
}
