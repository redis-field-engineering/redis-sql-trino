package com.redis.trino;

import com.redis.trino.RedisEnterprise.Deployment;

/**
 * {@link TestCursorReads} on a sharded database, whose coordinator holds the cursors.
 */
public class TestShardedCursorReads extends TestCursorReads {

	@Override
	protected Deployment deployment() {
		return Deployment.SHARDED;
	}
}
