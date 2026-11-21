package com.redis.trino;

public class TestShardedNativeSearch extends TestNativeSearch {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
