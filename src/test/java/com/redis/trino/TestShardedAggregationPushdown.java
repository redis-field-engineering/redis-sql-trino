package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

/**
 * The aggregation pushdown tests against a sharded database, through the connector's cluster client.
 */
public class TestShardedAggregationPushdown extends TestAggregationPushdown {

	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
