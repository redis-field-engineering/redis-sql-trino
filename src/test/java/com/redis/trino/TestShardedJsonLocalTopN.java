package com.redis.trino;

public class TestShardedJsonLocalTopN extends TestJsonLocalTopN {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
