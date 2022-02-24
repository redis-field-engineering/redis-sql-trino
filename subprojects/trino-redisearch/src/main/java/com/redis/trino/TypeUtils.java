package com.redis.trino;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.StandardTypes.JSON;

public final class TypeUtils {
	private TypeUtils() {
	}

	public static boolean isJsonType(Type type) {
		return type.getBaseName().equals(JSON);
	}

	public static boolean isArrayType(Type type) {
		return type instanceof ArrayType;
	}

	public static boolean isMapType(Type type) {
		return type instanceof MapType;
	}

	public static boolean isRowType(Type type) {
		return type instanceof RowType;
	}
}
