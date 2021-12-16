package com.redis.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
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
import io.airlift.slice.Slice;
import io.lettuce.core.RedisURI;
import io.redisearch.querybuilder.Node;
import io.redisearch.querybuilder.QueryBuilder;
import io.redisearch.querybuilder.Value;
import io.redisearch.querybuilder.Values;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
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

	public RediSearchSession(TypeManager typeManager, StatefulRedisModulesConnection<String, String> connection,
			RediSearchConfig config) {
		this.typeManager = requireNonNull(typeManager, "typeManager is null");
		this.connection = requireNonNull(connection, "connection is null");
		this.config = requireNonNull(config, "config is null");
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
		return loadTableSchema(tableName);
	}

	public void createTable(SchemaTableName name, List<RediSearchColumnHandle> columns) {
		createTableMetadata(name, columns);
	}

	public void dropTable(SchemaTableName tableName) {
		throw new UnsupportedOperationException("dropTable not implemented");
		// TODO
	}

	public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata) {
		throw new UnsupportedOperationException("addColumn not implemented");
		// TODO
	}

	private RediSearchTable loadTableSchema(SchemaTableName schemaTableName) throws TableNotFoundException {
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
		if (!connection.sync().list().contains(tableName)) {
			throw new TableNotFoundException(new SchemaTableName(schemaName, tableName),
					format("Index '%s' not found", tableName), null);
		}
		return RedisModulesUtils.indexInfo(connection.sync().indexInfo(tableName));
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

	public SearchResults<String, String> execute(RediSearchTableHandle tableHandle,
			List<RediSearchColumnHandle> columns) {
		String index = tableHandle.getSchemaTableName().getTableName();
		String query = buildQuery(tableHandle.getConstraint());
		Builder<String, String> options = SearchOptions.<String, String>builder();
		tableHandle.getLimit().ifPresent(num -> options.limit(Limit.of(0, num)));
		log.debug("Find documents: index: %s, query: %s", index, query);
		return connection.sync().search(index, query, options.build());
	}

	@VisibleForTesting
	static String buildQuery(TupleDomain<ColumnHandle> tupleDomain) {
		List<Node> nodes = new ArrayList<>();
		if (tupleDomain.getDomains().isPresent()) {
			for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
				RediSearchColumnHandle column = (RediSearchColumnHandle) entry.getKey();
				Optional<Node> predicate = buildPredicate(column, entry.getValue());
				predicate.ifPresent(nodes::add);
			}
		}
		if (nodes.isEmpty()) {
			return "*";
		}
		return QueryBuilder.intersect(nodes.toArray(new Node[0])).toString();
	}

	private static Optional<Node> buildPredicate(RediSearchColumnHandle column, Domain domain) {
		String name = column.getName();
		Type type = column.getType();
		List<Object> singleValues = new ArrayList<>();
		List<Node> disjuncts = new ArrayList<>();
		for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
			if (range.isSingleValue()) {
				Optional<Object> translated = translateValue(range.getSingleValue(), type);
				if (translated.isEmpty()) {
					return Optional.empty();
				}
				singleValues.add(translated.get());
			} else {
				List<Value> rangeConjuncts = new ArrayList<>();
				if (!range.isLowUnbounded()) {
					Optional<Object> translated = translateValue(range.getLowBoundedValue(), type);
					if (translated.isEmpty()) {
						return Optional.empty();
					}
					double number = ((Number) translated.get()).doubleValue();
					rangeConjuncts.add(range.isLowInclusive() ? Values.ge(number) : Values.gt(number));
				}
				if (!range.isHighUnbounded()) {
					Optional<Object> translated = translateValue(range.getHighBoundedValue(), type);
					if (translated.isEmpty()) {
						return Optional.empty();
					}
					double number = ((Number) translated.get()).doubleValue();
					rangeConjuncts.add(range.isHighInclusive() ? Values.le(number) : Values.lt(number));
				}
				// If rangeConjuncts is null, then the range was ALL, which should already have
				// been checked for
				verify(!rangeConjuncts.isEmpty());
				disjuncts.add(QueryBuilder.intersect(name, rangeConjuncts.toArray(new Value[0])));
			}
		}

		// Add back all of the possible single values either as an equality or an IN
		// predicate
		
		if (singleValues.size() == 1) {
			disjuncts.add(QueryBuilder.intersect(name, value(singleValues.get(0), type)));
		} else {
			List<Value> values = new ArrayList<>();
			for (Object trinoNativeValue : singleValues) {
				checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue),
						"%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(),
						type);
				values.add(value(trinoNativeValue, type));
			}
			disjuncts.add(QueryBuilder.intersect(name, values.toArray(new Value[0])));
		}

		return Optional.of(QueryBuilder.union(disjuncts.toArray(new Node[0])));
	}

	private static Value value(Object trinoNativeValue, Type type) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(type, "type is null");
		if (type == TINYINT) {
			return Values.eq((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
		}

		if (type == SMALLINT) {
			return Values.eq((long) Shorts.checkedCast(((Long) trinoNativeValue)));
		}

		if (type == IntegerType.INTEGER) {
			return Values.eq((long) toIntExact(((Long) trinoNativeValue)));
		}

		if (type == BIGINT) {
			return Values.eq((Long) trinoNativeValue);
		}
		if (type instanceof VarcharType) {
			// TODO introduce RediSearch Field type to know which to use (tag, text,
			// numeric)
			return Values.value((String) trinoNativeValue);

		}
		throw new UnsupportedOperationException("Type " + type + " not supported");
	}

	private static Optional<Object> translateValue(Object trinoNativeValue, Type type) {
		requireNonNull(trinoNativeValue, "trinoNativeValue is null");
		requireNonNull(type, "type is null");
		checkArgument(Primitives.wrap(type.getJavaType()).isInstance(trinoNativeValue),
				"%s (%s) is not a valid representation for %s", trinoNativeValue, trinoNativeValue.getClass(), type);

		if (type == TINYINT) {
			return Optional.of((long) SignedBytes.checkedCast(((Long) trinoNativeValue)));
		}

		if (type == SMALLINT) {
			return Optional.of((long) Shorts.checkedCast(((Long) trinoNativeValue)));
		}

		if (type == IntegerType.INTEGER) {
			return Optional.of((long) toIntExact(((Long) trinoNativeValue)));
		}

		if (type == BIGINT) {
			return Optional.of(trinoNativeValue);
		}
		if (type instanceof VarcharType) {
			return Optional.of(((Slice) trinoNativeValue).toStringUtf8());
		}

		return Optional.empty();
	}

	private Field buildField(RediSearchColumnHandle column) {
		Field.Type fieldType = toFieldType(column.getType());
		switch (fieldType) {
		case GEO:
			return Field.geo(column.getName()).build();
		case NUMERIC:
			return Field.numeric(column.getName()).build();
		case TAG:
			return Field.tag(column.getName()).build();
		case TEXT:
			return Field.text(column.getName()).build();
		}
		throw new IllegalArgumentException(String.format("Field type %s not supported", fieldType));
	}

	private static Field.Type toFieldType(Type type) {
		if (type.equals(BooleanType.BOOLEAN)) {
			return Field.Type.TAG;
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
		if (type instanceof VarcharType) {
			return Field.Type.TEXT;
		}
		if (type.equals(DateType.DATE)) {
			return Field.Type.TAG;
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

	private void createTableMetadata(SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns) {
		String tableName = schemaTableName.getTableName();
		if (!connection.sync().list().contains(tableName)) {
			List<Field> fields = columns.stream().filter(c -> !c.getName().equals("_id")).map(this::buildField)
					.collect(Collectors.toList());
			CreateOptions.Builder<String, String> options = CreateOptions.<String, String>builder();
			options.prefix(tableName + ":");
			connection.sync().create(tableName, options.build(), fields.toArray(new Field[0]));
		}
	}

}
