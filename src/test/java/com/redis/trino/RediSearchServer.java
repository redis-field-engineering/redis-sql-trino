package com.redis.trino;

import java.io.Closeable;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

public class RediSearchServer implements Closeable {

    // Redis 8 bundles the Query Engine (RediSearch), so no redis-stack image is needed.
    // search-max-aggregate-results defaults to unlimited in Redis 8, replacing the old
    // REDISEARCH_ARGS="MAXAGGREGATERESULTS -1" setting.
    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:8.4");

    private static final int REDIS_PORT = 6379;

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

    @Override
    public void close() {
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
        container.close();
    }

}
