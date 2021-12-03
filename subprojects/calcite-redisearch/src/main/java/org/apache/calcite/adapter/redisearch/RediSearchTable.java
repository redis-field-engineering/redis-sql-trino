package org.apache.calcite.adapter.redisearch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Order;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchOptions.Builder;
import com.redis.lettucemod.search.SearchResults;

/**
 * Table based on a RediSearch index.
 */
public class RediSearchTable extends AbstractQueryableTable implements TranslatableTable {

	private static final Logger log = LoggerFactory.getLogger(RediSearchTable.class);

	public static final int DEFAULT_LIMIT = 100;
	private final StatefulRedisModulesConnection<String, String> connection;
	private final IndexInfo indexInfo;
	private RelProtoDataType protoRowType;

	RediSearchTable(StatefulRedisModulesConnection<String, String> connection, IndexInfo indexInfo) {
		super(Object[].class);
		this.connection = connection;
		this.indexInfo = indexInfo;
	}

	@Override
	public String toString() {
		return "RedisSearchTable {" + indexInfo.getIndexName() + "}";
	}

	public Enumerable<Object> query(List<Map.Entry<String, Class<?>>> fields,
			List<Map.Entry<String, String>> selectFields, List<Map.Entry<String, String>> aggregateFunctions,
			List<String> groupByFields, List<String> predicates,
			List<Map.Entry<String, RelFieldCollation.Direction>> sort, Long offsetValue, Long limitValue) {
		final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
		for (Map.Entry<String, Class<?>> field : fields) {
			SqlTypeName typeName = typeFactory.createJavaType(field.getValue()).getSqlTypeName();
			RelDataType type;
			if (typeName == SqlTypeName.ARRAY) {
				type = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.ANY), -1);
			} else if (typeName == SqlTypeName.MULTISET) {
				type = typeFactory.createMultisetType(typeFactory.createSqlType(SqlTypeName.ANY), -1);
			} else if (typeName == SqlTypeName.MAP) {
				RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
				type = typeFactory.createMapType(anyType, anyType);
			} else {
				type = typeFactory.createSqlType(typeName);
			}
			fieldInfo.add(field.getKey(), type).nullable(true);
		}

		final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

		// TODO
//        ImmutableMap<String, String> aggFuncMap = ImmutableMap.of();
//        if (!aggregateFunctions.isEmpty()) {
//            ImmutableMap.Builder<String, String> aggFuncMapBuilder = ImmutableMap.builder();
//            for (Map.Entry<String, String> e : aggregateFunctions) {
//                aggFuncMapBuilder.put(e.getKey(), e.getValue());
//            }
//            aggFuncMap = aggFuncMapBuilder.build();
//        }

		// Combine all predicates conjunctively
		String query = predicates.isEmpty() ? "*" : Util.toString(predicates, "", " ", "");
		Builder<String, String> options = SearchOptions.builder();
		if (!groupByFields.isEmpty()) {
			throw new UnsupportedOperationException("GROUP BY not yet supported");
		}
		if (!sort.isEmpty()) {
			if (sort.size() > 1) {
				throw new UnsupportedOperationException("ORDER BY only supports a single field");
			}
			Map.Entry<String, RelFieldCollation.Direction> sortBy = sort.iterator().next();
			options.sortBy(SearchOptions.SortBy.<String, String>field(sortBy.getKey())
					.order(sortBy.getValue() == RelFieldCollation.Direction.ASCENDING ? Order.ASC : Order.DESC));
		}
		options.limit(SearchOptions.Limit.of(offsetValue == null ? 0 : offsetValue,
				limitValue == null ? DEFAULT_LIMIT : limitValue));
		Hook.QUERY_PLAN.run(query);
		log.info("RediSearch query: {}", query);

		return new AbstractEnumerable<Object>() {
			@Override
			public Enumerator<Object> enumerator() {
				try {
					SearchResults<String, String> results = connection.sync().search(indexInfo.getIndexName(), query,
							options.build());
					return new RediSearchEnumerator(results, resultRowType);
				} catch (Exception e) {
					String message = String.format(Locale.ROOT, "Failed to execute query [%s] on %s", query,
							indexInfo.getIndexName());
					throw new RuntimeException(message, e);
				}
			}
		};
	}

	public Map<String, Field.Type> indexFields() {
		Map<String, Field.Type> fieldTypes = new LinkedHashMap<>();
		indexInfo.getFields().forEach(f -> fieldTypes.put(f.getName(), f.getType()));
		return fieldTypes;
	}

	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
		return new RediSearchQueryable<>(queryProvider, schema, this, tableName);
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		final RelOptCluster cluster = context.getCluster();
		return new RediSearchTableScan(cluster, cluster.traitSetOf(RediSearchRel.CONVENTION), relOptTable, this, null);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType == null) {
			protoRowType = relDataType();
		}
		return protoRowType.apply(typeFactory);

	}

	private RelProtoDataType relDataType() {
		final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
		final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
		for (Field field : indexInfo.getFields()) {
			fieldInfo.add(field.getName(), typeFactory.createSqlType(sqlTypeName(field))).nullable(true);
		}
		return RelDataTypeImpl.proto(fieldInfo.build());
	}

	private SqlTypeName sqlTypeName(Field field) {
		if (field.getType() == Field.Type.NUMERIC) {
			return SqlTypeName.DOUBLE;
		}
		return SqlTypeName.VARCHAR;
	}

	/**
	 * Implementation of {@link Queryable} based on a {@link RediSearchTable}.
	 *
	 * @param <T> type
	 */
	public static class RediSearchQueryable<T> extends AbstractTableQueryable<T> {

		public RediSearchQueryable(QueryProvider queryProvider, SchemaPlus schema, RediSearchTable table,
				String tableName) {
			super(queryProvider, schema, table, tableName);
		}

		@Override
		public Enumerator<T> enumerator() {
			return null;
		}

		private RediSearchTable getTable() {
			return (RediSearchTable) this.table;
		}

		/**
		 * Called via code-generation.
		 */
		public Enumerable<Object> query(List<Map.Entry<String, Class<?>>> fields,
				List<Map.Entry<String, String>> selectFields, List<Map.Entry<String, String>> aggregateFunctions,
				List<String> groupByFields, List<String> predicates,
				List<Map.Entry<String, RelFieldCollation.Direction>> sort, Long offset, Long limit) {
			return getTable().query(fields, selectFields, aggregateFunctions, groupByFields, predicates, sort, offset,
					limit);
		}
	}
}
