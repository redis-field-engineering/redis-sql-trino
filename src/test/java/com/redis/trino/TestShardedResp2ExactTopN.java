package com.redis.trino;

public class TestShardedResp2ExactTopN extends TestResp2ExactTopN {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
