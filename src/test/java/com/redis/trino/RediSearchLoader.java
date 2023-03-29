package com.redis.trino;

import static com.google.common.base.Preconditions.checkState;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.github.f4b6a3.ulid.UlidFactory;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;

public class RediSearchLoader extends AbstractTestingTrinoClient<Void> {

	private final String tableName;
	private final StatefulRedisModulesConnection<String, String> connection;

	public RediSearchLoader(AbstractRedisClient client, String tableName, TestingTrinoServer trinoServer,
			Session defaultSession) {
		super(trinoServer, defaultSession);
		requireNonNull(client, "client is null");
		this.connection = RedisModulesUtils.connection(client);
		this.tableName = requireNonNull(tableName, "tableName is null");
	}

	@Override
	public ResultsSession<Void> getResultSession(Session session) {
		requireNonNull(session, "session is null");
		return new RediSearchLoadingSession();
	}

	@Override
	public void close() {
		connection.close();
		super.close();
	}

	private class RediSearchLoadingSession implements ResultsSession<Void> {

		private final AtomicReference<List<Type>> types = new AtomicReference<>();

		private RediSearchLoadingSession() {
		}

		@SuppressWarnings("unchecked")
		@Override
		public void addResults(QueryStatusInfo statusInfo, QueryData data) {
			if (types.get() == null && statusInfo.getColumns() != null) {
				types.set(getTypes(statusInfo.getColumns()));
			}

			if (data.getData() == null) {
				return;
			}
			checkState(types.get() != null, "Type information is missing");
			List<Column> columns = statusInfo.getColumns();
			if (!connection.sync().ftList().contains(tableName)) {
				List<Field<String>> schema = new ArrayList<>();
				for (int i = 0; i < columns.size(); i++) {
					Type type = types.get().get(i);
					schema.add(field(columns.get(i).getName(), type));
				}
				connection.sync().ftCreate(tableName,
						CreateOptions.<String, String>builder().prefix(tableName + ":").build(),
						schema.toArray(Field[]::new));
			}
			connection.setAutoFlushCommands(false);
			try {
				UlidFactory factory = UlidFactory.newInstance(new Random());
				List<RedisFuture<?>> futures = new ArrayList<>();
				for (List<Object> fields : data.getData()) {
					String key = tableName + ":" + factory.create().toString();
					Map<String, String> map = new HashMap<>();
					for (int i = 0; i < fields.size(); i++) {
						Type type = types.get().get(i);
						String value = convertValue(fields.get(i), type);
						map.put(columns.get(i).getName(), value);
					}
					futures.add(connection.async().hset(key, map));
				}
				connection.flushCommands();
				LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}

		private Field<String> field(String name, Type type) {
			if (type instanceof VarcharType) {
				return Field.tag(name).build();
			}
			if (type == BOOLEAN || type == DATE) {
				return Field.tag(name).build();
			}
			if (type == BIGINT || type == INTEGER || type == DOUBLE) {
				return Field.numeric(name).build();
			}
			throw new IllegalArgumentException("Unhandled type: " + type);
		}

		@Override
		public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties) {
			return null;
		}

		private String convertValue(Object value, Type type) {
			if (value == null) {
				return null;
			}
			if (type == BOOLEAN || type instanceof VarcharType) {
				return String.valueOf(value);
			}
			if (type == DATE) {
				return (String) value;
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
}
