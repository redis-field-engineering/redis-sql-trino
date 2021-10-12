package org.apache.calcite.adapter.redisearch;

import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.Utils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.IndexInfo;
import io.lettuce.core.RedisURI;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

/**
 * Schema mapped onto a RediSearch index.
 */
public class RediSearchSchema extends AbstractSchema {

    private final String index;
    private final StatefulRedisModulesConnection<String, String> connection;

    public RediSearchSchema(String uri, String index, String username, String password) {
        this.index = index;
        RedisURI redisURI = RedisURI.create(uri);
        if (username != null && password != null) {
            redisURI.setUsername(username);
            redisURI.setPassword(password.toCharArray());
        }
        RedisModulesClient client = RedisModulesClient.create(redisURI);
        this.connection = client.connect();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        builder.put(index, new RediSearchTable(this, index));
        return builder.build();
    }

    public StatefulRedisModulesConnection<String, String> getConnection() {
        return connection;
    }

    public IndexInfo getIndexInfo() {
        return Utils.indexInfo(connection.sync().indexInfo(index));
    }

    private SqlTypeName sqlTypeName(Field field) {
        if (field.getType() == Field.Type.NUMERIC) {
            return SqlTypeName.DOUBLE;
        }
        return SqlTypeName.VARCHAR;
    }

    public RelProtoDataType getRelDataType() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        IndexInfo indexInfo = getIndexInfo();
        for (Field field : indexInfo.getFields()) {
            fieldInfo.add(field.getName(), typeFactory.createSqlType(sqlTypeName(field))).nullable(true);
        }
        return RelDataTypeImpl.proto(fieldInfo.build());
    }
}
