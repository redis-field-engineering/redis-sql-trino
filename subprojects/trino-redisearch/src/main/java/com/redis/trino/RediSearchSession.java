package com.redis.trino;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchOptions.Builder;
import com.redis.lettucemod.search.SearchOptions.Limit;
import com.redis.lettucemod.search.SearchResults;

import io.airlift.log.Logger;
import io.lettuce.core.RedisURI;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

public class RediSearchSession {
	private static final Logger log = Logger.get(RediSearchSession.class);

	private final TypeManager typeManager;
	private final StatefulRedisModulesConnection<String, String> connection;
	private final RediSearchConfig config;
	private final LoadingCache<SchemaTableName, RediSearchTable> tableCache;

	public RediSearchSession(TypeManager typeManager, StatefulRedisModulesConnection<String, String> connection,
			RediSearchConfig config) {
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.connection = requireNonNull(connection, "connection is null");
		this.config = requireNonNull(config, "config is null");
		// TODO make table cache expiration configurable
		this.tableCache = CacheBuilder.newBuilder().expireAfterWrite(1, HOURS).refreshAfterWrite(1, MINUTES)
				.build(CacheLoader.from(this::loadTableSchema));
	}

	public StatefulRedisModulesConnection<String, String> getConnection() {
		return connection;
	}

	public RediSearchConfig getConfig() {
		return config;
	}

	public void shutdown() {
		connection.close();
	}

	public List<HostAddress> getAddresses() {
		if (config.getUri().isPresent()) {
			RedisURI redisURI = RedisURI.create(config.getUri().get());
			return Collections.singletonList(HostAddress.fromParts(redisURI.getHost(), redisURI.getPort()));
		}
		return Collections.emptyList();
	}

	public Set<String> getAllTables() throws SchemaNotFoundException {
		ImmutableSet.Builder<String> builder = ImmutableSet.builder();
		builder.addAll(connection.sync().list());
		return builder.build();
	}

	public RediSearchTable getTable(SchemaTableName tableName) throws TableNotFoundException {
		try {
			return tableCache.getUnchecked(tableName);
		} catch (UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TrinoException.class);
			throw e;
		}
	}

	public void createTable(SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns) {
		String tableName = schemaTableName.getTableName();
		if (!connection.sync().list().contains(tableName)) {
			List<Field> fields = columns.stream().filter(c -> !c.getName().equals("_id"))
					.map(c -> buildField(c.getName(), c.getType())).collect(Collectors.toList());
			CreateOptions.Builder<String, String> options = CreateOptions.<String, String>builder();
			options.prefix(tableName + ":");
			connection.sync().create(tableName, options.build(), fields.toArray(new Field[0]));
		}
	}

	public void dropTable(SchemaTableName tableName) {
		connection.sync().dropindexDeleteDocs(toRemoteTableName(tableName.getSchemaName(), tableName.getTableName()));
		tableCache.invalidate(tableName);
	}

	public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata) {
		String schemaName = schemaTableName.getSchemaName();
		String tableName = toRemoteTableName(schemaName, schemaTableName.getTableName());
		connection.sync().alter(tableName, buildField(columnMetadata.getName(), columnMetadata.getType()));
		tableCache.invalidate(schemaTableName);
	}

	private String toRemoteTableName(String schemaName, String tableName) {
		verify(tableName.equals(tableName.toLowerCase(ENGLISH)), "tableName not in lower-case: %s", tableName);
		if (!config.isCaseInsensitiveNameMatching()) {
			return tableName;
		}
		for (String remoteTableName : getAllTables()) {
			if (tableName.equals(remoteTableName.toLowerCase(ENGLISH))) {
				return remoteTableName;
			}
		}
		return tableName;
	}

	public void dropColumn(SchemaTableName schemaTableName, String columnName) {
		throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
	}

	private RediSearchTable loadTableSchema(SchemaTableName schemaTableName) {
		IndexInfo indexInfo = getIndexInfo(schemaTableName.getSchemaName(), schemaTableName.getTableName());

		ImmutableList.Builder<RediSearchColumnHandle> columnHandles = ImmutableList.builder();

		for (Field columnMetadata : indexInfo.getFields()) {
			RediSearchColumnHandle columnHandle = buildColumnHandle(columnMetadata);
			columnHandles.add(columnHandle);
		}

		RediSearchTableHandle tableHandle = new RediSearchTableHandle(schemaTableName);
		return new RediSearchTable(tableHandle, columnHandles.build());
	}

	private IndexInfo getIndexInfo(String schemaName, String tableName) throws TableNotFoundException {
		if (connection.sync().list().contains(tableName)) {
			return RedisModulesUtils.indexInfo(connection.sync().indexInfo(tableName));
		}
		throw new TableNotFoundException(new SchemaTableName(schemaName, tableName),
				format("Index '%s' not found", tableName), null);
	}

	private RediSearchColumnHandle buildColumnHandle(Field field) {
		return buildColumnHandle(field.getName(), field.getType(), false);
	}

	private RediSearchColumnHandle buildColumnHandle(String name, Field.Type type, boolean hidden) {
		return new RediSearchColumnHandle(name, columnType(type), hidden);
	}

	private Type columnType(Field.Type type) {
		return columnType(typeSignature(type));
	}

	private Type columnType(TypeSignature typeSignature) {
		return typeManager.fromSqlType(typeSignature.toString());
	}

	public SearchResults<String, String> execute(RediSearchTableHandle tableHandle) {
		String index = tableHandle.getSchemaTableName().getTableName();
		String query = RediSearchQueryBuilder.buildQuery(tableHandle.getConstraint());
		Builder<String, String> options = SearchOptions.<String, String>builder();
		options.limit(Limit.of(0,
				tableHandle.getLimit().isPresent() ? tableHandle.getLimit().getAsInt() : config.getDefaultLimit()));
		log.info("Find documents: index: %s, query: %s", index, query);
		return connection.sync().search(index, query, options.build());
	}

	private Field buildField(String columnName, Type columnType) {
		Field.Type fieldType = toFieldType(columnType);
		switch (fieldType) {
		case GEO:
			return Field.geo(columnName).build();
		case NUMERIC:
			return Field.numeric(columnName).build();
		case TAG:
			return Field.tag(columnName).build();
		case TEXT:
			return Field.text(columnName).build();
		}
		throw new IllegalArgumentException(String.format("Field type %s not supported", fieldType));
	}

	private static Field.Type toFieldType(Type type) {
		if (type.equals(BooleanType.BOOLEAN)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(BigintType.BIGINT)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(IntegerType.INTEGER)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(SmallintType.SMALLINT)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(TinyintType.TINYINT)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(DoubleType.DOUBLE)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(RealType.REAL)) {
			return Field.Type.NUMERIC;
		}
		if (type instanceof DecimalType) {
			return Field.Type.NUMERIC;
		}
		if (type instanceof VarcharType) {
			return Field.Type.TAG;
		}
		if (type instanceof CharType) {
			return Field.Type.TAG;
		}
		if (type.equals(DateType.DATE)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
			return Field.Type.NUMERIC;
		}
		if (type.equals(UuidType.UUID)) {
			return Field.Type.TAG;
		}
		throw new IllegalArgumentException("unsupported type: " + type);
	}

	private TypeSignature typeSignature(Field.Type type) {
		if (type == Field.Type.NUMERIC) {
			return doubleType();
		}
		return varcharType();
	}

	private TypeSignature doubleType() {
		return DOUBLE.getTypeSignature();
	}

	private TypeSignature varcharType() {
		return createUnboundedVarcharType().getTypeSignature();
	}

}
