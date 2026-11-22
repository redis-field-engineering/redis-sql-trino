package com.redis.trino;

public class TestShardedRelationalAssessment extends TestRelationalAssessment {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
