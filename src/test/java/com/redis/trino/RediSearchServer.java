package com.redis.trino;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * A database for one test class, on the Redis Enterprise cluster shared by the JVM.
 * <p>
 * {@link #getConnection()} is a plain (non-cluster) connection for test setup. A sharded database's shards are all on
 * the one node, so it serves every key, but scripts can only touch keys on a single shard.
 */
public class RediSearchServer implements Closeable {

    private static final Duration INDEXING_TIMEOUT = Duration.ofMinutes(1);

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

    private final RedisEnterprise cluster;

    private final RedisEnterprise.Database database;

    private final RedisClient client;

    private final StatefulRedisConnection<String, String> connection;

    public RediSearchServer() {
        this(Deployment.NON_SHARDED);
    }

    public RediSearchServer(Deployment deployment) {
        this.cluster = RedisEnterprise.shared();
        this.database = cluster.createDatabase(deployment);
        try {
            this.client = RedisClient.create(RedisURI.create(getRedisURI()));
            this.connection = client.connect();
        } catch (RuntimeException e) {
            cluster.deleteDatabase(database);
            throw e;
        }
    }

    public Deployment getDeployment() {
        return database.getDeployment();
    }

    public String getRedisURI() {
        return database.getRedisURI();
    }

    /**
     * @return the catalog properties that connect the connector to this database
     */
    public Map<String, String> getConnectorProperties() {
        return ImmutableMap.of("redisearch.uri", getRedisURI(), "redisearch.cluster",
                String.valueOf(getDeployment().isCluster()));
    }

    /**
     * Writes {@code count} hashes {@code <keyPrefix><i>} with an {@code id} field of {@code i}, from 1, pipelined.
     * Unlike a script, this works when the keys are on different shards.
     */
    public void writeHashes(String keyPrefix, int count) {
        // Its own connection: test methods run concurrently and share getConnection(), whose commands would sit
        // unsent while auto-flush is off
        try (StatefulRedisConnection<String, String> pipeline = client.connect()) {
            pipeline.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            for (int i = 1; i <= count; i++) {
                futures.add(pipeline.async().hset(keyPrefix + i, "id", String.valueOf(i)));
            }
            pipeline.flushCommands();
            LettuceFutures.awaitAll(pipeline.getTimeout(), futures.toArray(new RedisFuture[0]));
        }
    }

    public RedisClient getClient() {
        return client;
    }

    public StatefulRedisConnection<String, String> getConnection() {
        return connection;
    }

    public void awaitIndexed(String index) {
        awaitIndexed(connection.sync(), index);
    }

    /**
     * Waits for the background indexing that FT.CREATE starts on a populated keyspace to finish. Until then the
     * connector rejects queries on the index, and documents written meanwhile may not be searchable yet.
     */
    public static void awaitIndexed(RedisCommands<String, String> redis, String index) {
        long deadline = System.nanoTime() + INDEXING_TIMEOUT.toNanos();
        while (RediSearchIndexInfo.parse(redis.dispatch(FT_INFO, new NestedMultiOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).add(index))).isIndexing()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Index " + index + " still indexing after " + INDEXING_TIMEOUT);
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
        cluster.deleteDatabase(database);
    }

}
