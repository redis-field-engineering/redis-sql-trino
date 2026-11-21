package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

public class TestShardedAtomicHashWrites extends TestAtomicHashWrites {
	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
