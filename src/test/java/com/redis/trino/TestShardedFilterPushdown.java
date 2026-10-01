package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

/**
 * The filter pushdown tests against a sharded database, through the connector's cluster client.
 */
public class TestShardedFilterPushdown extends TestFilterPushdown {

	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
