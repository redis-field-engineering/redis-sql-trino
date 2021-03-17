package com.redislabs.jdbc.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.redislabs.lettusearch.IndexInfo;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Schema mapped onto a RediSearch Region.
 */
public class RediSearchSchema extends AbstractSchema {

    final StatefulRediSearchConnection<String, String> connection;
    private final List<String> indexNames;
    private ImmutableMap<String, Table> tableMap;

    public RediSearchSchema(StatefulRediSearchConnection<String, String> connection, final Iterable<String> indexNames) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.indexNames = ImmutableList.copyOf(Objects.requireNonNull(indexNames, "indexNames"));
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            for (String indexName : indexNames) {
                IndexInfo<String> info = RediSearchUtils.getInfo(connection.sync().ftInfo(indexName));
                Table table = new RediSearchTable(info);
                builder.put(indexName, table);
            }
            tableMap = builder.build();
        }

        return tableMap;
    }
}
