package com.redis.trino;

public class TestShardedViewAcceptance extends TestViewAcceptance {
    @Override
    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.SHARDED;
    }
}
