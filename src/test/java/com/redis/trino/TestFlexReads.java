package com.redis.trino;

/** Runs the complete cursor-free read contract against actual Redis Software Flex, not a RAM simulation. */
public class TestFlexReads extends TestCursorFreeReads {
    @Override
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.FLEX; }
}
