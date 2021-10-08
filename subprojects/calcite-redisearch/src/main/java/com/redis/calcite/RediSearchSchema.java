package com.redis.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.Utils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.search.IndexInfo;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Schema mapped onto a RediSearch index.
 */
public class RediSearchSchema extends AbstractSchema {

    private final StatefulRedisModulesConnection<String, String> connection;
    private final List<String> indexNames;
    private ImmutableMap<String, Table> tableMap;

    public RediSearchSchema(StatefulRedisModulesConnection<String, String> connection, final Iterable<String> indexNames) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.indexNames = ImmutableList.copyOf(Objects.requireNonNull(indexNames, "indexNames"));
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            for (String indexName : indexNames) {
                IndexInfo info = Utils.indexInfo(connection.sync().indexInfo(indexName));
                Table table = new RediSearchTable(info);
                builder.put(indexName, table);
            }
            tableMap = builder.build();
        }

        return tableMap;
    }

    public StatefulRedisModulesConnection<String, String> getConnection() {
        return connection;
    }

    public List<String> getIndexNames() {
        return indexNames;
    }

}
