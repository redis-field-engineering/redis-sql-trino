package com.redis.trino;

public class TestShardedSqlRelationMetadata extends TestSqlRelationMetadata {
    @Override
    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.SHARDED;
    }
}
