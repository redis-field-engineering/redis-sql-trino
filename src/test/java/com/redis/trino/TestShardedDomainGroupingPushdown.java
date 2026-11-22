package com.redis.trino;

public class TestShardedDomainGroupingPushdown extends TestDomainGroupingPushdown {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
