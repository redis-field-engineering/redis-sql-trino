package com.redis.trino;

import java.io.Closeable;

import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;

public class RediSearchServer implements Closeable {

	private final RedisModulesContainer dockerContainer;
	private final RedisTestContext context;

	public RediSearchServer() {
		this.dockerContainer = new RedisModulesContainer(
				RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));
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
