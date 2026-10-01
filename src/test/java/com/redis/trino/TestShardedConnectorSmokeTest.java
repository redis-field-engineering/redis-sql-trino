package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

/**
 * The smoke tests against a sharded database, through the connector's cluster client.
 */
public class TestShardedConnectorSmokeTest extends TestConnectorSmokeTest {

	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
