package com.redis.trino;

import java.io.Closeable;

import org.testcontainers.utility.DockerImageName;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RediSearchServer implements Closeable {

    private static final String TAG = "6.2.6-v9";

    private static final DockerImageName DOCKER_IMAGE_NAME = RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(TAG);

    private final RedisStackContainer container = new RedisStackContainer(DOCKER_IMAGE_NAME).withEnv("REDISEARCH_ARGS",
            "MAXAGGREGATERESULTS -1");

    private final AbstractRedisClient client;

    private final StatefulRedisModulesConnection<String, String> connection;

    public RediSearchServer() {
        this.container.start();
        this.client = ClientBuilder.create(RedisURI.create(container.getRedisURI())).cluster(container.isCluster()).build();
        this.connection = RedisModulesUtils.connection(client);
    }

    public String getRedisURI() {
        return container.getRedisURI();
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
