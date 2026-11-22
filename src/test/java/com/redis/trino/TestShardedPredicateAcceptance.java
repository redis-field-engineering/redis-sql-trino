package com.redis.trino;

public class TestShardedPredicateAcceptance extends TestPredicateAcceptance {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.SHARDED; }
}
