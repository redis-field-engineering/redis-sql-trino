package com.redis.trino;

import java.io.Closeable;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class RediSearchServer implements Closeable {

    private final RedisStackContainer container = new RedisStackContainer(
            RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG)).withEnv("REDISEARCH_ARGS",
                    "MAXAGGREGATERESULTS -1");

    private final AbstractRedisClient client;

    private final StatefulRedisModulesConnection<String, String> connection;

    public RediSearchServer() {
        this.container.start();
        RedisURI uri = RedisURI.create(container.getRedisURI());
        this.client = container.isCluster() ? RedisModulesClusterClient.create(uri) : RedisModulesClient.create(uri);
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
