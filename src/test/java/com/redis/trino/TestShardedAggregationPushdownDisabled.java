package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

public class TestShardedAggregationPushdownDisabled extends TestAggregationPushdownDisabled {

	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
