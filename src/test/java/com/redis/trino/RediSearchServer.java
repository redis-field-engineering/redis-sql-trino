package com.redis.trino;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.google.common.collect.ImmutableMap;
import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
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

    private final GenericContainer<?> openSource;

    private final RedisClient client;

    private final StatefulRedisConnection<String, String> connection;

    public RediSearchServer() {
        this(Deployment.NON_SHARDED);
    }

    public RediSearchServer(Deployment deployment) {
        String ossImage = System.getProperty("redisearch.test.oss-image");
        if (ossImage != null) {
            if (deployment != Deployment.NON_SHARDED) {
                throw new IllegalArgumentException("redisearch.test.oss-image only supports NON_SHARDED test classes");
            }
            this.cluster = null;
            this.database = null;
            this.openSource = new GenericContainer<>(DockerImageName.parse(ossImage)).withExposedPorts(6379);
            try {
                openSource.start();
            } catch (RuntimeException | Error e) {
                try {
                    openSource.close();
                } catch (RuntimeException | Error cleanup) {
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
        } else {
            this.openSource = null;
            this.cluster = deployment == Deployment.FLEX ? RedisEnterprise.sharedFlex() : RedisEnterprise.shared();
            this.database = cluster.createDatabase(deployment);
        }
        RedisClient connectingClient = null;
        StatefulRedisConnection<String, String> connected;
        try {
            connectingClient = RedisClient.create(RedisURI.create(getRedisURI()));
            connected = connectingClient.connect();
        } catch (RuntimeException | Error e) {
            RedisClient failedClient = connectingClient;
            try {
                if (failedClient == null) {
                    stopBackend();
                } else {
                    closeAll(failedClient::shutdown, () -> failedClient.getResources().shutdown(), this::stopBackend);
                }
            } catch (RuntimeException | Error cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        this.client = connectingClient;
        this.connection = connected;
    }

    public Deployment getDeployment() {
        return openSource == null ? database.getDeployment() : Deployment.NON_SHARDED;
    }

    public String getRedisURI() {
        if (openSource != null) {
            return RedisURI.Builder.redis(openSource.getHost(), openSource.getMappedPort(6379)).build().toString();
        }
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

    /**
     * One key {@code <keyPrefix><n>} for each of {@code shards}, on that shard of a sharded database: shards are
     * numbered from 0 in the order of the lowest slot they own. A database without the OSS Cluster API has one shard,
     * which every key is on.
     */
    @SuppressWarnings("unchecked")
    public List<String> keysOnShards(String keyPrefix, int... shards) {
        // The shard of each slot
        int[] slotShards = new int[SlotHash.SLOT_COUNT];
        int shardCount = 1;
        if (getDeployment().isCluster()) {
            List<List<Object>> ranges = new ArrayList<>();
            for (Object range : connection.sync().clusterSlots()) {
                ranges.add((List<Object>) range);
            }
            ranges.sort(Comparator.comparingLong(range -> (Long) range.get(0)));
            // The master's ID, or its address
            Map<Object, Integer> masters = new LinkedHashMap<>();
            for (List<Object> range : ranges) {
                List<Object> master = (List<Object>) range.get(2);
                Object id = master.size() > 2 ? master.get(2) : master.subList(0, 2);
                int shard = masters.computeIfAbsent(id, unused -> masters.size());
                for (long slot = (Long) range.get(0); slot <= (Long) range.get(1); slot++) {
                    slotShards[(int) slot] = shard;
                }
            }
            shardCount = masters.size();
        }
        List<String> keys = new ArrayList<>();
        int next = 1;
        for (int shard : shards) {
            String key = keyPrefix + next++;
            while (slotShards[SlotHash.getSlot(key)] != shard % shardCount) {
                key = keyPrefix + next++;
            }
            keys.add(key);
        }
        return keys;
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
        closeAll(connection::close, client::shutdown, () -> client.getResources().shutdown(), this::stopBackend);
    }

    /** Attempt every cleanup, preserving the first failure and suppressing later ones. */
    private static void closeAll(Runnable... steps) {
        Throwable failure = null;
        for (Runnable step : steps) {
            try {
                step.run();
            } catch (RuntimeException | Error e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private void stopBackend() {
        if (openSource != null) {
            openSource.close();
        } else {
            cluster.deleteDatabase(database);
        }
    }

}
