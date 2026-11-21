package com.redis.trino;

public class TestShardedExactDistinctPushdown extends TestExactDistinctPushdown {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
