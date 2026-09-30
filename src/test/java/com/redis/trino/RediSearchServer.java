package com.redis.trino;

import java.io.Closeable;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RediSearchServer implements Closeable {

    // Redis 8 bundles the Query Engine (RediSearch), so no redis-stack image is needed.
    // search-max-aggregate-results defaults to unlimited in Redis 8, replacing the old
    // REDISEARCH_ARGS="MAXAGGREGATERESULTS -1" setting.
    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:8.4");

    private static final int REDIS_PORT = 6379;

    private final GenericContainer<?> container = new GenericContainer<>(REDIS_IMAGE)
            .withExposedPorts(REDIS_PORT)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    private final AbstractRedisClient client;

    private final StatefulRedisModulesConnection<String, String> connection;

    public RediSearchServer() {
        this.container.start();
        RedisModulesClient redisClient = RedisModulesClient.create(RedisURI.create(getRedisURI()));
        this.client = redisClient;
        this.connection = redisClient.connect();
    }

    public String getRedisURI() {
        return "redis://" + container.getHost() + ":" + container.getMappedPort(REDIS_PORT);
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public StatefulRedisModulesConnection<String, String> getConnection() {
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
