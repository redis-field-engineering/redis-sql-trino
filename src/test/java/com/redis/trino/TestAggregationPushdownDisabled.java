package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

public class TestAggregationPushdownDisabled extends AbstractTestQueryFramework {

	protected Deployment deployment() {
		return Deployment.NON_SHARDED;
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
		var redis = server.getConnection().sync();
		redis.ftCreate("amounts", CreateArgs.builder().withPrefix("amount:").build(),
				List.of(TagFieldArgs.builder().name("g").build(), NumericFieldArgs.builder().name("x").build(),
						NumericFieldArgs.builder().name("y").build()));
		server.awaitIndexed("amounts");
		redis.hset("amount:1", Map.of("g", "a", "x", "2", "y", "3"));
		redis.hset("amount:2", Map.of("g", "a", "x", "4"));
		redis.hset("amount:3", Map.of("g", "b", "y", "5"));
		return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
				Map.of("redisearch.aggregation-pushdown.enabled", "false", "redisearch.cursor-count", "1"));
	}

	@Test
	public void testGlobalAggregationStaysInTrino() {
		assertThat(query("SELECT count(*), sum(x), avg(x), min(x), max(x) FROM amounts"))
				.isNotFullyPushedDown(AggregationNode.class)
				.matches("VALUES (BIGINT '3', DOUBLE '6', DOUBLE '3', DOUBLE '2', DOUBLE '4')");
	}

	@Test
	public void testGroupedAggregationAndNulls() {
		assertThat(query("SELECT g, count(*), sum(x), avg(x) FROM amounts GROUP BY g"))
				.isNotFullyPushedDown(AggregationNode.class)
				.matches("VALUES (VARCHAR 'a', BIGINT '2', DOUBLE '6', DOUBLE '3'), "
						+ "(VARCHAR 'b', BIGINT '1', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
	}

	@Test
	public void testArithmeticAggregationStaysInTrino() {
		assertThat(query("SELECT sum(x * y), avg(x + y) FROM amounts"))
				.isNotFullyPushedDown(AggregationNode.class).matches("VALUES (DOUBLE '6', DOUBLE '5')");
	}

	@Test
	public void testFilteringStillPushesDown() {
		assertThat(query("SELECT x FROM amounts WHERE x = 2")).isFullyPushedDown().matches("VALUES DOUBLE '2'");
		assertThat(query("SELECT count(*) FROM amounts WHERE x = 2"))
				.isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '1'");
	}
}
