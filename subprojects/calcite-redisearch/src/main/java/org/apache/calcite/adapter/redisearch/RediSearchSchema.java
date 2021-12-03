package org.apache.calcite.adapter.redisearch;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.IndexInfo;

/**
 * Schema mapped onto a RediSearch index.
 */
public class RediSearchSchema extends AbstractSchema {

    private final StatefulRedisModulesConnection<String, String> connection;
    private final Map<String, Table> tableMap;

    public RediSearchSchema(StatefulRedisModulesConnection<String, String> connection, String index) {
        this.connection = connection;
        if (index == null) {
            this.tableMap = this.createTables(this.indicesFromRediSearch());
        } else {
            this.tableMap = this.createTables(Collections.singletonList(index));
        }
    }

    private List<String> indicesFromRediSearch() {
        return connection.sync().list();
    }

    private Map<String, Table> createTables(List<String> indices) {
        ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        for (String index : indices) {
            builder.put(index, new RediSearchTable(connection, indexInfo(index)));
        }

        return builder.build();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return this.tableMap;
    }

    private IndexInfo indexInfo(String index) {
        return RedisModulesUtils.indexInfo(connection.sync().indexInfo(index));
    }

}
