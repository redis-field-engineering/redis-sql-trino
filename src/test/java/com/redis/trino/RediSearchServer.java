package com.redis.trino;

import java.io.Closeable;

import com.redis.testcontainers.RedisStackContainer;
import com.redis.testcontainers.junit.RedisTestContext;

public class RediSearchServer implements Closeable {

	private final RedisStackContainer dockerContainer;
	private final RedisTestContext context;

	public RediSearchServer() {
		this.dockerContainer = new RedisStackContainer(
				RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG))
				.withEnv("REDISEARCH_ARGS", "MAXAGGREGATERESULTS 1000000");
		this.dockerContainer.start();
		this.context = new RedisTestContext(dockerContainer);
	}

	public RedisTestContext getTestContext() {
		return context;
	}

	@Override
	public void close() {
		context.close();
		dockerContainer.close();
	}
}
