package com.redis.trino;

public class TestShardedFiniteGroupingBounds extends TestFiniteGroupingBounds {
    @Override
    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.SHARDED;
    }
}
