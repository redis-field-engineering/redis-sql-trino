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

import static com.google.common.base.Throwables.throwIfUnchecked;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.redis.trino.RediSearchTranslator.Aggregation;

import io.airlift.log.Logger;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Builder;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RediSearchAsyncCommands;
import io.lettuce.core.api.sync.RediSearchCommands;
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
import io.lettuce.core.search.SearchReply;
import io.lettuce.core.search.arguments.AggregateArgs;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.FieldArgs;
import io.lettuce.core.search.arguments.GeoFieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.QueryDialects;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
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
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;

public class RediSearchSession {

    private static final Logger log = Logger.get(RediSearchSession.class);

    private final TypeManager typeManager;

    private final RediSearchConfig config;

    private final RediSearchTranslator translator;

    // TAG fields created here split values on the ASCII unit separator rather than the default ',', which SQL strings
    // often contain. Redis can't query a value split into several tags, so Trino would evaluate the filter alone.
    private static final String TAG_SEPARATOR = "\u001f";

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

    private final RediSearchTableCache tableCache;

    // Describes cached tables again
    private final ThreadPoolExecutor tableRefresher;

    // Opened by the first scans to use them
    private final Connection[] scanConnections;

    private final AtomicInteger nextScanConnection = new AtomicInteger();

    private final boolean resp3;

    private static final int TABLE_REFRESH_THREADS = 2;

    // Whether APPLY steps can use case(), once a probe has evaluated it
    private volatile Boolean caseSupported;

    public RediSearchSession(TypeManager typeManager, RediSearchConfig config) {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(config, "config is null");
        this.translator = new RediSearchTranslator(config);
        this.client = client(config);
        Connection primary = connect();
        this.connection = primary.connection;
        this.sync = primary.sync;
        this.async = primary.async;
        this.resp3 = isResp3(primary.connection);
        this.scanConnections = new Connection[Math.toIntExact(config.getScanConnections())];
        this.tableRefresher = new ThreadPoolExecutor(TABLE_REFRESH_THREADS, TABLE_REFRESH_THREADS, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("redisearch-table-refresh-%s").build());
        tableRefresher.allowCoreThreadTimeOut(true);
        this.tableCache = new RediSearchTableCache(Duration.ofSeconds(config.getTableCacheRefresh()),
                this::loadTableSchema,
                tableName -> listIndexNames().contains(toRemoteTableName(tableName.getTableName())), tableRefresher,
                Ticker.systemTicker());
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
        // Asynchronous commands, such as the cursor reads scans prefetch, time out like synchronous ones
        builder.timeoutOptions(TimeoutOptions.enabled());
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

    /**
     * A connection, and its commands.
     */
    public static final class Connection {
        private final StatefulConnection<String, String> connection;
        private final RedisClusterCommands<String, String> sync;
        private final RedisClusterAsyncCommands<String, String> async;

        private Connection(StatefulConnection<String, String> connection, RedisClusterCommands<String, String> sync,
                RedisClusterAsyncCommands<String, String> async) {
            this.connection = connection;
            this.sync = sync;
            this.async = async;
        }
    }

    private Connection connect() {
        if (client instanceof RedisClusterClient) {
            StatefulRedisClusterConnection<String, String> clusterConnection = ((RedisClusterClient) client).connect();
            return new Connection(clusterConnection, clusterConnection.sync(), clusterConnection.async());
        }
        StatefulRedisConnection<String, String> redisConnection = ((RedisClient) client).connect();
        return new Connection(redisConnection, redisConnection.sync(), redisConnection.async());
    }

    /**
     * The connection for a scan, which reads all of its batches over it. Scans take turns on
     * {@code redisearch.scan-connections} connections, so that one scan's batches don't hold up the others' commands,
     * and more than one of the client's I/O threads reads the replies.
     */
    public Connection scanConnection() {
        int index = Math.floorMod(nextScanConnection.getAndIncrement(), scanConnections.length);
        synchronized (scanConnections) {
            if (scanConnections[index] == null) {
                scanConnections[index] = connect();
            }
            return scanConnections[index];
        }
    }

    public StatefulConnection<String, String> getConnection() {
        return connection;
    }

    /**
     * Whether the connections negotiated RESP3. Over RESP2, the shards of a sharded database send its coordinator the
     * doubles they compute rounded to 12 significant digits.
     */
    public boolean isResp3() {
        return resp3;
    }

    /**
     * Whether Redis evaluates {@code case()} in APPLY steps. Redis 8.2 and later do; Redis 8.0 and Redis Stack 7.4
     * have it only with unstable features enabled, and fail a query only once they evaluate it on a document. So the
     * probe evaluates it on one of the index's documents; if the index has none, it tells nothing and is tried again
     * next time.
     */
    public boolean isCaseSupported(String index) {
        Boolean supported = caseSupported;
        if (supported != null) {
            return supported;
        }
        try {
            AggregationReply<String> reply = sync.ftAggregate(index, "*",
                    AggregateArgs.builder().apply("case(1, 1, 0)", "__probe").limit(0, 1)
                            .dialect(QueryDialects.DIALECT2).build());
            if (reply.getReplies().stream().anyMatch(searchReply -> !searchReply.getResults().isEmpty())) {
                caseSupported = true;
                return true;
            }
        } catch (RedisCommandExecutionException e) {
            // "Unknown function name 'case'", or unavailable without unstable features
            if (e.getMessage() != null && e.getMessage().contains("case")) {
                log.info("Redis can't evaluate case(), so Redis computes sums and averages with SUM and AVG: %s",
                        e.getMessage());
                caseSupported = false;
            } else {
                log.warn(e, "Could not tell whether Redis evaluates case()");
            }
        }
        return false;
    }

    private static boolean isResp3(StatefulConnection<String, String> connection) {
        StatefulConnection<String, String> negotiated = connection;
        if (connection instanceof StatefulRedisClusterConnection<String, String> cluster) {
            // A cluster connection doesn't expose the protocol it negotiated, but its connections to the nodes do
            negotiated = cluster.getConnection(cluster.getPartitions().iterator().next().getNodeId());
        }
        return negotiated instanceof StatefulRedisConnectionImpl<?, ?> redisConnection
                && redisConnection.getConnectionState().getNegotiatedProtocolVersion() == ProtocolVersion.RESP3;
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
        tableRefresher.shutdownNow();
        synchronized (scanConnections) {
            for (Connection scanConnection : scanConnections) {
                if (scanConnection != null) {
                    scanConnection.connection.close();
                }
            }
        }
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
        return tableCache.get(tableName);
    }

    public Set<String> getAllTables() {
        return listIndexNames().stream().collect(toSet());
    }

    public void createTable(SchemaTableName schemaTableName, List<RediSearchColumnHandle> columns) {
        String index = schemaTableName.getTableName();
        if (!sync.ftList().contains(index)) {
            Map<String, Type> types = new LinkedHashMap<>();
            columns.stream().filter(c -> !RediSearchBuiltinField.isKeyColumn(c.getName()))
                    .forEach(c -> types.put(c.getName(), c.getType()));
            List<FieldArgs> fields = types.entrySet().stream().map(c -> buildField(c.getKey(), c.getValue()))
                    .collect(Collectors.toList());
            // Before the index, so its columns never read back without their types
            RediSearchColumnTypes.write(sync, index, types);
            try {
                // A new table starts empty and its rows are indexed as they're written. Without SKIPINITIALSCAN,
                // FT.CREATE scans the whole keyspace in the background, and queries on the table fail until that
                // finishes.
                sync.ftCreate(index, CreateArgs.builder().withPrefix(index + ":").skipInitialScan().build(), fields);
            } catch (RuntimeException e) {
                RediSearchColumnTypes.delete(sync, index);
                throw e;
            }
        }
    }

    /**
     * Whether Redis can evaluate a column's domain in the table's query, which then matches every row in the domain,
     * and maybe others.
     */
    public boolean canQuery(RediSearchTableHandle table, RediSearchColumnHandle column, Domain domain) {
        return column.isSupportsPredicates() && RediSearchQueryBuilder.isSupported(column, domain)
                && !hasCustomStopwords(table, column) && !hasJsonBooleanValue(table, column, domain);
    }

    // A TAG field indexes a JSON boolean as the tag true or false, but the connector reads it as 1 or 0, as DIALECT 2
    // loads it: a tag query for 1 or 0 wouldn't match it
    private boolean hasJsonBooleanValue(RediSearchTableHandle table, RediSearchColumnHandle column, Domain domain) {
        return column.getFieldType() == RediSearchFieldType.TAG
                && getTable(table.getSchemaTableName()).getIndexInfo().getKeyType()
                        .filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent()
                && RediSearchQueryBuilder.tagValues(domain.getValues()).orElseThrow().stream()
                        .map(value -> RediSearchQueryBuilder.tagValue(column.getType(), value))
                        .anyMatch(value -> value.equals("1") || value.equals("0"));
    }

    // TEXT queries drop the default stop words, which would match nothing; with its own list, a value's remaining terms
    // could all be stop words
    private boolean hasCustomStopwords(RediSearchTableHandle table, RediSearchColumnHandle column) {
        return column.getFieldType() == RediSearchFieldType.TEXT
                && getTable(table.getSchemaTableName()).getIndexInfo().hasCustomStopwords();
    }

    /**
     * Fails unless rows written to the table can be read back. The connector writes hashes, which an index on JSON
     * documents never sees; DELETE works on both.
     */
    public void verifyWritable(SchemaTableName tableName) {
        RediSearchTable table = getTable(tableName);
        if (table.getIndexInfo().getKeyType().filter(type -> type == RediSearchIndexInfo.KeyType.JSON).isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "Index %s is on JSON documents; the connector only writes hashes, so only DELETE is supported",
                    table.getTableHandle().getIndex()));
        }
    }

    public void dropTable(SchemaTableName tableName) {
        String index = toRemoteTableName(tableName.getTableName());
        sync.ftDropindex(index, true);
        RediSearchColumnTypes.delete(sync, index);
        tableCache.invalidate(tableName);
    }

    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata columnMetadata) {
        String tableName = toRemoteTableName(schemaTableName.getTableName());
        FieldArgs field = buildField(columnMetadata.getName(), columnMetadata.getType());
        // Also for an index created outside Trino, whose other columns keep the types read from FT.INFO
        RediSearchColumnTypes.add(sync, tableName, columnMetadata.getName(), columnMetadata.getType());
        sync.ftAlter(tableName, List.of(field));
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
        }
        Map<String, String> declaredTypes = RediSearchColumnTypes.read(sync, index);
        for (RediSearchIndexInfo.Field indexedField : indexInfo.getFields()) {
            RediSearchColumnHandle column = buildColumnHandle(indexedField, indexInfo.getKeyType(),
                    Optional.ofNullable(declaredTypes.get(indexedField.getAttribute())));
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
                        false, Optional.empty()));
                fields.add(docField);
            }
        }
        // Hidden columns go last. Trino's MERGE planning indexes the visible columns by their position among all of
        // them, so a hidden column first made WHEN NOT MATCHED THEN INSERT fail when it left out the last column.
        for (RediSearchBuiltinField builtinfield : RediSearchBuiltinField.values()) {
            columns.add(builtinfield.getColumnHandle());
        }
        RediSearchTableHandle tableHandle = new RediSearchTableHandle(schemaTableName, index);
        return new RediSearchTable(tableHandle, columns.build(), indexInfo);
    }

    private Optional<RediSearchIndexInfo> indexInfo(String index) {
        return indexInfo(sync, index);
    }

    private static Optional<RediSearchIndexInfo> indexInfo(RedisClusterCommands<String, String> sync, String index) {
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

    private RediSearchColumnHandle buildColumnHandle(RediSearchIndexInfo.Field field,
            Optional<RediSearchIndexInfo.KeyType> keyType, Optional<String> declaredType) {
        RediSearchFieldType type = field.getType();
        // FILTER compares a hash's TAG or TEXT value as stored, and with DIALECT 2 the first of a JSON document's values
        // at the path, a boolean as 1 or 0: the values the connector reads. Scans read JSON documents with DIALECT 3,
        // whose values FILTER can't compare, so the connector keeps their equal rows itself.
        boolean filterable = keyType.isPresent()
                && (type == RediSearchFieldType.TAG || type == RediSearchFieldType.TEXT);
        return new RediSearchColumnHandle(field.getAttribute(), columnType(field, declaredType), type, false, true,
                field.getSeparator(), filterable);
    }

    /**
     * The type the column was created with, if the connector saved it and its field still has the type the connector
     * creates for it: an index dropped and created again outside Trino may have other fields by the same names.
     * Otherwise DOUBLE for NUMERIC fields and VARCHAR for the others.
     */
    private Type columnType(RediSearchIndexInfo.Field field, Optional<String> declaredType) {
        if (declaredType.isPresent()) {
            try {
                Type type = typeManager.getType(TypeId.of(declaredType.get()));
                if (toFieldType(type) == field.getType()) {
                    return type;
                }
            } catch (RuntimeException e) {
                log.warn(e, "Ignoring type %s saved for column %s", declaredType.get(), field.getAttribute());
            }
        }
        if (field.getType() == RediSearchFieldType.NUMERIC) {
            return DOUBLE;
        }
        return createUnboundedVarcharType();
    }

    /**
     * A batch of aggregation rows, each with its columns' values in the reader's column order, and the cursor to read
     * the next batch with, if there are more. In cluster mode the cursor also names the node that holds it, which
     * reads and deletes are sent to.
     */
    public static class AggregateResult {
        private final List<String[]> rows;
        private final Optional<Cursor> cursor;
        private final RediSearchRowReader reader;

        public AggregateResult(List<String[]> rows, Optional<Cursor> cursor, RediSearchRowReader reader) {
            this.rows = rows;
            this.cursor = cursor;
            this.reader = reader;
        }

        public List<String[]> getRows() {
            return rows;
        }

        public Optional<Cursor> getCursor() {
            return cursor;
        }

        /**
         * @return how to read the rows of the cursor's next batches
         */
        public RediSearchRowReader getReader() {
            return reader;
        }
    }

    public AggregateResult aggregate(Connection scan, RediSearchTableHandle table, List<RediSearchColumnHandle> columns) {
        Optional<RediSearchIndexInfo> indexInfo = indexInfo(scan.sync, table.getIndex());
        indexInfo.ifPresent(info -> verifyIndexed(table.getIndex(), info));
        Aggregation aggregation = translator.aggregate(table, columns, indexInfo);
        log.debug("Running %s", aggregation);
        AggregateResult result = result(scan, aggregation.getReader(), Optional.empty(),
                scan.sync.ftAggregate(aggregation.getIndex(), aggregation.getQuery(), aggregation.getArgs()));
        // A batch can come back empty while the cursor still has rows, so the aggregation is only empty once the
        // cursor is exhausted
        while (result.getRows().isEmpty() && result.getCursor().isPresent()) {
            Cursor cursor = result.getCursor().get();
            result = result(scan, result.getReader(), Optional.of(cursor), cursorCommands(scan, cursor).read(table, cursor));
        }
        if (result.getRows().isEmpty() && aggregation.isGlobal()) {
            // A global aggregation over no documents still returns one row. With GROUP BY terms there are no groups,
            // so no rows.
            return new AggregateResult(List.<String[]>of(result.getReader().emptyAggregation()), Optional.empty(),
                    result.getReader());
        }
        return result;
    }

    // While Redis indexes existing documents in the background (e.g. after FT.CREATE on a populated keyspace), queries
    // return only the documents indexed so far, with no warning in the reply
    private static void verifyIndexed(String index, RediSearchIndexInfo info) {
        if (info.isIndexing()) {
            throw new TrinoException(REDISEARCH_INDEX_NOT_READY, format(ENGLISH,
                    "Index %s is still being built (%.0f%% indexed), so its results would be incomplete; retry once indexing finishes",
                    index, info.getPercentIndexed() * 100));
        }
    }

    /**
     * Starts reading the cursor's next batch. Pass the reply to {@link #result} to read its rows, on the caller's
     * thread rather than the connection's, which {@link #result} may wait on.
     */
    public CompletableFuture<AggregationReply<String>> cursorReadAsync(Connection scan, RediSearchTableHandle table,
            Cursor cursor) {
        return cursorCommands(scan, cursor).readAsync(table, cursor);
    }

    /**
     * @param cursor the cursor the reply was read from, if any
     */
    public AggregateResult result(Connection scan, RediSearchRowReader reader, Optional<Cursor> cursor,
            AggregationReply<String> reply) {
        List<String[]> rows = new ArrayList<>();
        List<CompletableFuture<?>> exactReads = new ArrayList<>();
        for (SearchReply<String> searchReply : reply.getReplies()) {
            for (SearchReply.SearchResult<String> result : searchReply.getResults()) {
                if (!reader.matches(result.getFields())) {
                    continue;
                }
                String[] row = reader.read(result.getFields());
                for (int position : reader.getExactPositions()) {
                    if (row[position] != null && RediSearchRowReader.isPossiblyRounded(row[position])) {
                        // Pipelined, and rare: only values of 2^53 or more
                        String key = result.getFields().get(RediSearchBuiltinField.KEY.getName()).asString();
                        exactReads.add(scan.async.hget(key, reader.getExactField(position)).toCompletableFuture()
                                .thenAccept(value -> row[position] = value));
                    }
                }
                rows.add(row);
            }
        }
        if (!exactReads.isEmpty()) {
            try {
                CompletableFuture.allOf(exactReads.toArray(CompletableFuture[]::new)).join();
            } catch (CompletionException e) {
                throwIfUnchecked(e.getCause());
                throw e;
            }
        }
        // Once the values are exact
        rows.replaceAll(reader::project);
        // Cursor ID 0 means there are no more rows
        Optional<Cursor> next = reply.getCursor().filter(c -> c.getCursorId() != 0);
        // The cursor stays on the node that created it
        next.filter(c -> c.getNodeId().isEmpty())
                .ifPresent(c -> cursor.flatMap(Cursor::getNodeId).ifPresent(c::setNodeId));
        return new AggregateResult(rows, next, reader);
    }

    /**
     * Cursor commands for the connection that holds a cursor.
     */
    private class CursorCommands {
        private final RediSearchCommands<String> sync;
        private final RediSearchAsyncCommands<String> async;

        CursorCommands(RediSearchCommands<String> sync, RediSearchAsyncCommands<String> async) {
            this.sync = sync;
            this.async = async;
        }

        AggregationReply<String> read(RediSearchTableHandle table, Cursor cursor) {
            if (config.getCursorCount() > 0) {
                return sync.ftCursorread(table.getIndex(), cursor, Math.toIntExact(config.getCursorCount()));
            }
            return sync.ftCursorread(table.getIndex(), cursor);
        }

        CompletableFuture<AggregationReply<String>> readAsync(RediSearchTableHandle table, Cursor cursor) {
            if (config.getCursorCount() > 0) {
                return async.ftCursorread(table.getIndex(), cursor, Math.toIntExact(config.getCursorCount()))
                        .toCompletableFuture();
            }
            return async.ftCursorread(table.getIndex(), cursor).toCompletableFuture();
        }
    }

    private FieldArgs buildField(String columnName, Type columnType) {
        RediSearchFieldType fieldType = toFieldType(columnType);
        switch (fieldType) {
            case GEO:
                return GeoFieldArgs.builder().name(columnName).build();
            case NUMERIC:
                return NumericFieldArgs.builder().name(columnName).build();
            case TAG:
                // Case-sensitive like SQL, so tag queries return fewer rows for Trino to drop
                return TagFieldArgs.builder().name(columnName).separator(TAG_SEPARATOR).caseSensitive().build();
            case TEXT:
                return TextFieldArgs.builder().name(columnName).build();
            case GEOSHAPE:
            case VECTOR:
                throw new UnsupportedOperationException(fieldType + " field not supported");
        }
        throw new IllegalArgumentException(String.format("Field type %s not supported", fieldType));
    }

    /**
     * The index field type for a column, which must index the values {@link RediSearchPageSink#value} writes for it.
     */
    public static RediSearchFieldType toFieldType(Type type) {
        // Written as "true" and "false"
        if (type.equals(BooleanType.BOOLEAN)) {
            return RediSearchFieldType.TAG;
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
        // Written as ISO dates, e.g. 2024-01-02
        if (type.equals(DateType.DATE)) {
            return RediSearchFieldType.TAG;
        }
        // Timestamps are written as epoch milliseconds
        if (type.equals(TimestampType.TIMESTAMP_MILLIS)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            return RediSearchFieldType.NUMERIC;
        }
        if (type.equals(UuidType.UUID)) {
            return RediSearchFieldType.TAG;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    // A cursor lives on the node that ran FT.AGGREGATE. Lettuce's cluster client would route cursor commands there over
    // a read connection, which it opens with READONLY, a command Redis Enterprise doesn't support; so they go over the
    // node's primary connection instead.
    @SuppressWarnings("unchecked")
    private CursorCommands cursorCommands(Connection scan, Cursor cursor) {
        if (scan.connection instanceof StatefulRedisClusterConnection && cursor.getNodeId().isPresent()) {
            StatefulRedisConnection<String, String> node = ((StatefulRedisClusterConnection<String, String>) scan.connection)
                    .getConnection(cursor.getNodeId().get());
            return new CursorCommands(node.sync(), node.async());
        }
        return new CursorCommands(scan.sync, scan.async);
    }

    /**
     * Deletes the cursor without waiting for Redis, e.g. from a callback on the connection's thread, where waiting
     * would block the connection.
     */
    public void cursorDeleteAsync(Connection scan, RediSearchTableHandle tableHandle, Cursor cursor) {
        cursorCommands(scan, cursor).async.ftCursordel(tableHandle.getIndex(), cursor).exceptionally(e -> {
            log.warn(e, "Could not delete cursor %s of index %s", cursor.getCursorId(), tableHandle.getIndex());
            return null;
        });
    }

    /**
     * Opens a connection of the caller's own to write on. Its commands wait in the connection's buffer until
     * {@link Writer#flush}, which the connection other queries share can't do.
     */
    public Writer openWriter() {
        Connection writer = connect();
        writer.connection.setAutoFlushCommands(false);
        return new Writer(writer.connection, writer.async);
    }

    /**
     * Pipelines writes on a connection of its own.
     */
    public static final class Writer implements AutoCloseable {
        private final StatefulConnection<String, String> connection;
        private final RedisClusterAsyncCommands<String, String> commands;

        private Writer(StatefulConnection<String, String> connection, RedisClusterAsyncCommands<String, String> commands) {
            this.connection = connection;
            this.commands = commands;
        }

        /**
         * @return commands that wait in the connection's buffer until {@link #flush}
         */
        public RedisClusterAsyncCommands<String, String> commands() {
            return commands;
        }

        /**
         * Sends the buffered commands, then waits for their replies. Fails if any command does.
         */
        public void flush(List<RedisFuture<?>> futures) {
            connection.flushCommands();
            if (!LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]))) {
                throw new RedisCommandTimeoutException("Writes didn't complete within " + connection.getTimeout());
            }
        }

        @Override
        public void close() {
            connection.close();
        }
    }

}
