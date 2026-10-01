/*
 * MIT License
 *
 * Copyright (c) 2022, Redis Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.redis.trino;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.redis.trino.RediSearchErrorCode.REDISEARCH_INDEX_NOT_READY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.redis.trino.RediSearchTranslator.Aggregation;
import com.redis.trino.RediSearchTranslator.Search;

import io.airlift.log.Logger;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.lettuce.core.search.FieldValue;
import io.lettuce.core.search.SearchReply;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.FieldArgs;
import io.lettuce.core.search.arguments.GeoFieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.cache.EvictableCacheBuilder;
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
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

public class RediSearchSession {

    private static final Logger log = Logger.get(RediSearchSession.class);

    private final TypeManager typeManager;

    private final RediSearchConfig config;

    private final RediSearchTranslator translator;

    // FT.INFO is not part of Lettuce's RediSearch API
    private static final ProtocolKeyword FT_INFO = new ProtocolKeyword() {
        private final byte[] bytes = "FT.INFO".getBytes(StandardCharsets.US_ASCII);

        @Override
        public byte[] getBytes() {
            return bytes;
        }

        @Override
        public String toString() {
            return "FT.INFO";
        }
    };

    private final AbstractRedisClient client;

    private final StatefulConnection<String, String> connection;

    private final RedisClusterCommands<String, String> sync;

    private final RedisClusterAsyncCommands<String, String> async;

    private final Cache<SchemaTableName, RediSearchTable> tableCache;

    public RediSearchSession(TypeManager typeManager, RediSearchConfig config) {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(config, "config is null");
        this.translator = new RediSearchTranslator(config);
        this.client = client(config);
        if (client instanceof RedisClusterClient) {
            StatefulRedisClusterConnection<String, String> clusterConnection = ((RedisClusterClient) client).connect();
            this.connection = clusterConnection;
            this.sync = clusterConnection.sync();
            this.async = clusterConnection.async();
        } else {
            StatefulRedisConnection<String, String> redisConnection = ((RedisClient) client).connect();
            this.connection = redisConnection;
            this.sync = redisConnection.sync();
            this.async = redisConnection.async();
        }
        this.tableCache = EvictableCacheBuilder.newBuilder().expireAfterWrite(config.getTableCacheRefresh(), TimeUnit.SECONDS)
                .build();
    }

    private AbstractRedisClient client(RediSearchConfig config) {
        RedisURI redisURI = redisURI(config);
        if (config.isCluster()) {
            RedisClusterClient clusterClient = RedisClusterClient.create(redisURI);
            clusterClient.setOptions(ClusterClientOptions.builder(clientOptions(config)).build());
            return clusterClient;
        }
        RedisClient redisClient = RedisClient.create(redisURI);
        redisClient.setOptions(clientOptions(config));
        return redisClient;
    }

    private ClientOptions clientOptions(RediSearchConfig config) {
        ClientOptions.Builder builder = ClientOptions.builder();
        builder.sslOptions(sslOptions(config));
        builder.protocolVersion(protocolVersion(config));
        return builder.build();
    }

    private ProtocolVersion protocolVersion(RediSearchConfig config) {
        if (config.isResp2()) {
            return ProtocolVersion.RESP2;
        }
        return ClientOptions.DEFAULT_PROTOCOL_VERSION;
    }

    public SslOptions sslOptions(RediSearchConfig config) {
        Builder ssl = SslOptions.builder();
        if (!isNullOrEmpty(config.getKeyPath())) {
            ssl.keyManager(new File(config.getCertPath()), new File(config.getKeyPath()),
                    config.getKeyPassword().toCharArray());
        }
        if (!isNullOrEmpty(config.getCaCertPath())) {
            ssl.trustManager(new File(config.getCaCertPath()));
        }
        return ssl.build();
    }
    
    private static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    private RedisURI redisURI(RediSearchConfig config) {
        RedisURI.Builder uri = RedisURI.builder(RedisURI.create(config.getUri()));
        if (!isNullOrEmpty(config.getPassword())) {
            if (!isNullOrEmpty(config.getUsername())) {
                uri.withAuthentication(config.getUsername(), config.getPassword());
            } else {
                uri.withPassword(config.getPassword().toCharArray());
            }
        }
        if (config.isInsecure()) {
            uri.withVerifyPeer(false);
        }
        return uri.build();
    }

    public StatefulConnection<String, String> getConnection() {
        return connection;
    }

    public RedisClusterCommands<String, String> sync() {
        return sync;
    }

    public RedisClusterAsyncCommands<String, String> async() {
        return async;
    }

    public RediSearchConfig getConfig() {
        return config;
    }

    public void shutdown() {
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
    }

    public List<HostAddress> getAddresses() {
        RedisURI redisURI = RedisURI.create(config.getUri());
        return Collections.singletonList(HostAddress.fromParts(redisURI.getHost(), redisURI.getPort()));
    }

    private Set<String> listIndexNames() throws SchemaNotFoundException {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(sync.ftList());
        return builder.build();
    }

    /**
     * 
     * @param schemaTableName SchemaTableName to load
     * @return RediSearchTable describing the RediSearch index
     * @throws TableNotFoundException if no index by that name was found
     */
    public RediSearchTable getTable(SchemaTableName tableName) throws TableNotFoundException {
        try {
            return tableCache.get(tableName, () -> loadTableSchema(tableName));
        } catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e);
        }
    }

    public Set<String> getAllTables() {
        return listIndexNames().stream().collect(toSet());
    }

    public void createTable(SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns) {
        String index = schemaTableName.getTableName();
        if (!sync.ftList().contains(index)) {
            List<FieldArgs> fields = columns.stream().filter(c -> !RediSearchBuiltinField.isKeyColumn(c.getName()))
                    .map(c -> buildField(c.getName(), c.getType())).collect(Collectors.toList());
            // A new table starts empty and its rows are indexed as they're written. Without SKIPINITIALSCAN, FT.CREATE
            // scans the whole keyspace in the background, and queries on the table fail until that finishes.
            sync.ftCreate(index, CreateArgs.builder().withPrefix(index + ":").skipInitialScan().build(), fields);
        }
    }

    public void dropTable(SchemaTableName tableName) {
        sync.ftDropindex(toRemoteTableName(tableName.getTableName()), true);
        tableCache.invalidate(tableName);
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata) {
        String tableName = toRemoteTableName(schemaTableName.getTableName());
        sync.ftAlter(tableName, List.of(buildField(columnMetadata.getName(), columnMetadata.getType())));
        tableCache.invalidate(schemaTableName);
    }

    private String toRemoteTableName(String tableName) {
        verify(tableName.equals(tableName.toLowerCase(ENGLISH)), "tableName not in lower-case: %s", tableName);
        if (!config.isCaseInsensitiveNames()) {
            return tableName;
        }
        for (String remoteTableName : listIndexNames()) {
            if (tableName.equals(remoteTableName.toLowerCase(ENGLISH))) {
                return remoteTableName;
            }
        }
        return tableName;
    }

    public void dropColumn(SchemaTableName schemaTableName, String columnName) {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    /**
     * 
     * @param schemaTableName SchemaTableName to load
     * @return RediSearchTable describing the RediSearch index
     * @throws TableNotFoundException if no index by that name was found
     */
    private RediSearchTable loadTableSchema(SchemaTableName schemaTableName) throws TableNotFoundException {
        String index = toRemoteTableName(schemaTableName.getTableName());
        Optional<RediSearchIndexInfo> indexInfoOptional = indexInfo(index);
        if (indexInfoOptional.isEmpty()) {
            throw new TableNotFoundException(schemaTableName, format("Index '%s' not found", index), null);
        }
        RediSearchIndexInfo indexInfo = indexInfoOptional.get();
        Set<String> fields = new HashSet<>();
        ImmutableList.Builder<RediSearchColumnHandle> columns = ImmutableList.builder();
        for (RediSearchBuiltinField builtinfield : RediSearchBuiltinField.values()) {
            fields.add(builtinfield.getName());
            columns.add(builtinfield.getColumnHandle());
        }
        for (RediSearchIndexInfo.Field indexedField : indexInfo.getFields()) {
            RediSearchColumnHandle column = buildColumnHandle(indexedField);
            fields.add(column.getName());
            columns.add(column);
        }
        SearchReply<String> results = sync.ftSearch(index, "*");
        for (SearchReply.SearchResult<String> doc : results.getResults()) {
            for (String docField : doc.getFields().keySet()) {
                if (fields.contains(docField)) {
                    continue;
                }
                columns.add(new RediSearchColumnHandle(docField, VarcharType.VARCHAR, RediSearchFieldType.TEXT, false,
                        false));
                fields.add(docField);
            }
        }
        RediSearchTableHandle tableHandle = new RediSearchTableHandle(schemaTableName, index);
        return new RediSearchTable(tableHandle, columns.build(), indexInfo);
    }

    private Optional<RediSearchIndexInfo> indexInfo(String index) {
        try {
            List<Object> indexInfoList = sync.dispatch(FT_INFO, new NestedMultiOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).add(index));
            if (indexInfoList != null) {
                return Optional.of(RediSearchIndexInfo.parse(indexInfoList));
            }
        } catch (Exception e) {
            // Ignore as index might not exist
        }
        return Optional.empty();
    }

    private RediSearchColumnHandle buildColumnHandle(RediSearchIndexInfo.Field field) {
        RediSearchFieldType type = field.getType();
        return new RediSearchColumnHandle(field.getAttribute(), columnType(type), type, false, true);
    }

    private Type columnType(RediSearchFieldType type) {
        if (type == RediSearchFieldType.NUMERIC) {
            return DOUBLE;
        }
        return createUnboundedVarcharType();
    }

    public SearchReply<String> search(RediSearchTableHandle tableHandle, String[] columns) {
        Search search = translator.search(tableHandle, columns);
        log.info("Running %s", search);
        return sync.ftSearch(search.getIndex(), search.getQuery(), search.getArgs());
    }

    /**
     * A batch of aggregation rows and the cursor to read the next batch with (0 when there are no more).
     */
    public static class AggregateResult {
        private final List<Map<String, String>> rows;
        private final long cursor;

        public AggregateResult(List<Map<String, String>> rows, long cursor) {
            this.rows = rows;
            this.cursor = cursor;
        }

        public List<Map<String, String>> getRows() {
            return rows;
        }

        public long getCursor() {
            return cursor;
        }
    }

    public AggregateResult aggregate(RediSearchTableHandle table, String[] columnNames) {
        verifyIndexed(table.getIndex());
        Aggregation aggregation = translator.aggregate(table, columnNames);
        log.info("Running %s", aggregation);
        AggregateResult result = result(sync.ftAggregate(aggregation.getIndex(), aggregation.getQuery(),
                aggregation.getArgs()));
        // A batch can come back empty while the cursor still has rows, so the aggregation is only empty once the
        // cursor is exhausted
        while (result.getRows().isEmpty() && result.getCursor() != 0) {
            result = cursorRead(table, result.getCursor());
        }
        if (result.getRows().isEmpty() && aggregation.isGlobal()) {
            // A global aggregation over no documents still returns one row: count is 0 and the other metrics are null.
            // With GROUP BY terms there are no groups, so no rows.
            Map<String, String> row = new HashMap<>();
            for (RediSearchAggregation metric : table.getMetricAggregations()) {
                if (RediSearchAggregation.COUNT.equals(metric.getFunctionName())) {
                    row.put(metric.getAlias(), "0");
                }
            }
            return new AggregateResult(List.of(row), 0);
        }
        return result;
    }

    // While Redis indexes existing documents in the background (e.g. after FT.CREATE on a populated keyspace), queries
    // return only the documents indexed so far, with no warning in the reply
    private void verifyIndexed(String index) {
        indexInfo(index).filter(RediSearchIndexInfo::isIndexing).ifPresent(info -> {
            throw new TrinoException(REDISEARCH_INDEX_NOT_READY, format(ENGLISH,
                    "Index %s is still being built (%.0f%% indexed), so its results would be incomplete; retry once indexing finishes",
                    index, info.getPercentIndexed() * 100));
        });
    }

    public AggregateResult cursorRead(RediSearchTableHandle tableHandle, long cursor) {
        String index = tableHandle.getIndex();
        Cursor id = Cursor.of(cursor, null);
        if (config.getCursorCount() > 0) {
            return result(sync.ftCursorread(index, id, Math.toIntExact(config.getCursorCount())));
        }
        return result(sync.ftCursorread(index, id));
    }

    private static AggregateResult result(AggregationReply<String> reply) {
        List<Map<String, String>> rows = new ArrayList<>();
        for (SearchReply<String> searchReply : reply.getReplies()) {
            for (SearchReply.SearchResult<String> result : searchReply.getResults()) {
                Map<String, String> row = new HashMap<>();
                for (Map.Entry<String, FieldValue> field : result.getFields().entrySet()) {
                    FieldValue value = field.getValue();
                    if (value != null && !value.isNull()) {
                        row.put(field.getKey(), value.asString());
                    }
                }
                rows.add(row);
            }
        }
        long cursor = reply.getCursor().map(Cursor::getCursorId).orElse(0L);
        return new AggregateResult(rows, cursor);
    }

    private FieldArgs buildField(String columnName, Type columnType) {
        RediSearchFieldType fieldType = toFieldType(columnType);
        switch (fieldType) {
            case GEO:
                return GeoFieldArgs.builder().name(columnName).build();
            case NUMERIC:
                return NumericFieldArgs.builder().name(columnName).build();
            case TAG:
                return TagFieldArgs.builder().name(columnName).build();
            case TEXT:
                return TextFieldArgs.builder().name(columnName).build();
            case GEOSHAPE:
            case VECTOR:
                throw new UnsupportedOperationException(fieldType + " field not supported");
        }
        throw new IllegalArgumentException(String.format("Field type %s not supported", fieldType));
    }

    public static RediSearchFieldType toFieldType(Type type) {
        if (type.equals(BooleanType.BOOLEAN)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(BigintType.BIGINT)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(TinyintType.TINYINT)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(RealType.REAL)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type instanceof DecimalType) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type instanceof VarcharType) {
            return RediSearchFieldType.TAG;
        }
        if (type instanceof CharType) {
            return RediSearchFieldType.TAG;
        }
        if (type.equals(DateType.DATE)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(UuidType.UUID)) {
            return RediSearchFieldType.TAG;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public void cursorDelete(RediSearchTableHandle tableHandle, long cursor) {
        sync.ftCursordel(tableHandle.getIndex(), Cursor.of(cursor, null));
    }

    public Long deleteDocs(List<String> docIds) {
        return sync.del(docIds.toArray(String[]::new));
    }

}
