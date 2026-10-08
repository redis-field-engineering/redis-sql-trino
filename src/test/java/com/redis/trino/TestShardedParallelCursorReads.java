package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

public class TestShardedParallelCursorReads extends TestParallelCursorReads {
    @Override
    protected Deployment deployment() { return Deployment.SHARDED; }
}
