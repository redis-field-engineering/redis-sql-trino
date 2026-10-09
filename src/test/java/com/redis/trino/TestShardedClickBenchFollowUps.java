package com.redis.trino;

public class TestShardedClickBenchFollowUps extends TestClickBenchFollowUps {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
