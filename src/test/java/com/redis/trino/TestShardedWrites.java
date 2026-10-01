package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

/**
 * The write tests against a sharded database, through the connector's cluster client.
 */
public class TestShardedWrites extends TestWrites {

	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
