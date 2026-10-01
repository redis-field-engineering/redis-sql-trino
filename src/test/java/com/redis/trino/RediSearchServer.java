package com.redis.trino;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

public class RediSearchServer implements Closeable {

    // Redis 8 bundles the Query Engine (RediSearch), so no redis-stack image is needed.
    // search-max-aggregate-results defaults to unlimited in Redis 8, replacing the old
    // REDISEARCH_ARGS="MAXAGGREGATERESULTS -1" setting.
    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:8.4");

    private static final int REDIS_PORT = 6379;

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

    private final GenericContainer<?> container = new GenericContainer<>(REDIS_IMAGE)
            .withExposedPorts(REDIS_PORT)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private final RedisClient client;

    private final StatefulRedisConnection<String, String> connection;

    public RediSearchServer() {
        this.container.start();
        this.client = RedisClient.create(RedisURI.create(getRedisURI()));
        this.connection = client.connect();
    }

    public String getRedisURI() {
        return "redis://" + container.getHost() + ":" + container.getMappedPort(REDIS_PORT);
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
        container.close();
    }

}
