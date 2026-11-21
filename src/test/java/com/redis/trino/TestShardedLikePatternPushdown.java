package com.redis.trino;

public class TestShardedLikePatternPushdown extends TestLikePatternPushdown {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
