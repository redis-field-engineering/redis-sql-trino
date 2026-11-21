package com.redis.trino;

public class TestShardedResp2JsonLocalTopN extends TestResp2JsonLocalTopN {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
