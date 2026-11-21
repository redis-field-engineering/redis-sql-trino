package com.redis.trino;

public class TestShardedExactLocalTopN extends TestExactLocalTopN {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
