package com.redis.trino;

public class TestShardedExpressionCountPushdown extends TestExpressionCountPushdown {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
