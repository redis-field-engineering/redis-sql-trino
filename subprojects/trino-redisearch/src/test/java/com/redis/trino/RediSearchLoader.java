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
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;

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
	private final RedisTestContext context;

	public RediSearchLoader(RedisTestContext context, String tableName, TestingTrinoServer trinoServer,
			Session defaultSession) {
		super(trinoServer, defaultSession);

		this.tableName = requireNonNull(tableName, "tableName is null");
		this.context = requireNonNull(context, "client is null");
	}

	@Override
	public ResultsSession<Void> getResultSession(Session session) {
		requireNonNull(session, "session is null");
		return new RediSearchLoadingSession();
	}

	private class RediSearchLoadingSession implements ResultsSession<Void> {

		private final AtomicReference<List<Type>> types = new AtomicReference<>();

		private RediSearchLoadingSession() {
		}

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
			if (!context.sync().list().contains(tableName)) {
				List<Field> schema = new ArrayList<>();
				for (int i = 0; i < columns.size(); i++) {
					Type type = types.get().get(i);
					schema.add(field(columns.get(i).getName(), type));
				}
				context.sync().create(tableName,
						CreateOptions.<String, String>builder().prefix(tableName + ":").build(),
						schema.toArray(new Field[0]));
			}
			RedisModulesAsyncCommands<String, String> async = context.async();
			async.setAutoFlushCommands(false);
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
				futures.add(async.hset(key, map));
			}
			async.flushCommands();
			LettuceFutures.awaitAll(context.getConnection().getTimeout(), futures.toArray(new RedisFuture[0]));
			async.setAutoFlushCommands(true);
		}

		private Field field(String name, Type type) {
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
