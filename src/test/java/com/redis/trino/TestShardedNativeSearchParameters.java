package com.redis.trino;

public class TestShardedNativeSearchParameters extends TestNativeSearchParameters {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
