package com.redis.trino;

import static io.trino.spi.type.VarcharType.VARCHAR;

import io.trino.spi.type.Type;

enum RediSearchBuiltinField {

	KEY("__key", VARCHAR, RediSearchFieldType.TAG);

	private final String name;
	private final Type type;
	private final RediSearchFieldType fieldType;

	RediSearchBuiltinField(String name, Type type, RediSearchFieldType fieldType) {
		this.name = name;
		this.type = type;
		this.fieldType = fieldType;
	}

	public String getName() {
		return name;
	}

	public RediSearchColumnHandle getColumnHandle() {
		return new RediSearchColumnHandle(name, type, fieldType, true, false);
	}

	public static boolean isKeyColumn(String columnName) {
		return KEY.name.equals(columnName);
	}
}
