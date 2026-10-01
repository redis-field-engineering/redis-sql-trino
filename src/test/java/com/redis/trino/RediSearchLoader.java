package com.redis.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.github.f4b6a3.ulid.UlidFactory;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.FieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

/**
 * Loads a query result into Redis hashes under an index named after the table.
 */
public class RediSearchLoader implements AutoCloseable {

	private final String tableName;
	private final StatefulRedisConnection<String, String> connection;

	public RediSearchLoader(RedisClient client, String tableName) {
		requireNonNull(client, "client is null");
		this.connection = client.connect();
		this.tableName = requireNonNull(tableName, "tableName is null");
	}

	public void load(MaterializedResult result) {
		List<String> columns = result.getColumnNames();
		List<Type> types = result.getTypes();
		if (!connection.sync().ftList().contains(tableName)) {
			List<FieldArgs> schema = new ArrayList<>();
			for (int i = 0; i < columns.size(); i++) {
				schema.add(field(columns.get(i), types.get(i)));
			}
			connection.sync().ftCreate(tableName, CreateArgs.builder().withPrefix(tableName + ":").build(), schema);
			// FT.CREATE scans the existing keyspace in the background; rows written after it finishes are indexed
			// synchronously, so they're queryable as soon as load() returns
			RediSearchServer.awaitIndexed(connection.sync(), tableName);
		}
		connection.setAutoFlushCommands(false);
		try {
			UlidFactory factory = UlidFactory.newInstance(new Random());
			List<RedisFuture<?>> futures = new ArrayList<>();
			for (MaterializedRow row : result.getMaterializedRows()) {
				String key = tableName + ":" + factory.create().toString();
				Map<String, String> map = new HashMap<>();
				for (int i = 0; i < row.getFieldCount(); i++) {
					String value = convertValue(row.getField(i), types.get(i));
					if (value != null) {
						map.put(columns.get(i), value);
					}
				}
				futures.add(connection.async().hset(key, map));
			}
			connection.flushCommands();
			LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
		} finally {
			connection.setAutoFlushCommands(true);
		}
	}

	@Override
	public void close() {
		connection.close();
	}

	// The field types CREATE TABLE uses for these column types, so tests read values the way the connector writes them
	private static FieldArgs field(String name, Type type) {
		switch (RediSearchSession.toFieldType(type)) {
		case TAG:
			return TagFieldArgs.builder().name(name).build();
		case NUMERIC:
			return NumericFieldArgs.builder().name(name).build();
		default:
			throw new IllegalArgumentException("Unhandled type: " + type);
		}
	}

	private static String convertValue(Object value, Type type) {
		if (value == null) {
			return null;
		}
		if (type == BOOLEAN || type instanceof VarcharType || type == DATE) {
			// DATE is materialized as java.time.LocalDate, whose toString() is ISO-8601
			return String.valueOf(value);
		}
		if (type == BIGINT) {
			return String.valueOf(((Number) value).longValue());
		}
		if (type == INTEGER) {
			return String.valueOf(((Number) value).intValue());
		}
		if (type == DOUBLE) {
			return String.valueOf(((Number) value).doubleValue());
		}
		throw new IllegalArgumentException("Unhandled type: " + type);
	}
}
