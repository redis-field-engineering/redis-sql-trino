package com.redis.trino;

public class TestShardedCanonicalGrouping extends TestCanonicalGrouping {
    @Override
    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.SHARDED;
    }
}
