package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * Over RESP2, the shards of a sharded database send its coordinator the doubles they compute rounded to 12
 * significant digits, so Trino computes the aggregations whose results or keys are DOUBLE.
 */
public class TestResp2AggregationPushdown extends AbstractTestQueryFramework {

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		RediSearchServer redisearch = closeAfterClass(new RediSearchServer(Deployment.NON_SHARDED));
		TestAggregationPushdown.createNumbers(redisearch);
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, List.of(), Map.of(),
				Map.of("redisearch.resp2", "true"));
	}

	@Test
	public void testDoublesAggregatedByTrino() {
		assertAggregatedByTrino("SELECT sum(d), max(d) FROM numbers WHERE style = 'Wheat'",
				"VALUES (DOUBLE '0.1234567890123456' + DOUBLE '229577310901.21', DOUBLE '229577310901.21')");
		assertAggregatedByTrino("SELECT d, count(*) FROM numbers GROUP BY d", "VALUES (DOUBLE '0.1234567890123456', BIGINT '1'), "
				+ "(DOUBLE '0.1234567890123457', BIGINT '1'), (DOUBLE '229577310901.21', BIGINT '1'), "
				+ "(CAST(NULL AS DOUBLE), BIGINT '1'), (DOUBLE '-1.0000000000000002', BIGINT '1'), "
				+ "(DOUBLE '2.2250738585072014E-308', BIGINT '1'), (DOUBLE '1.7976931348623157E308', BIGINT '1')");
		// avg's result is a DOUBLE
		assertThat(query("SELECT style, count(*), avg(d) FROM numbers GROUP BY style"))
				.isNotFullyPushedDown(AggregationNode.class);
	}

	private void assertAggregatedByTrino(String sql, String expected) {
		assertThat(query(sql)).isNotFullyPushedDown(AggregationNode.class);
		RediSearchQueryRunner.assertExactRows(getQueryRunner(), sql, expected);
	}

	@Test
	public void testCountsPushedDown() {
		assertThat(query("SELECT count(*) FROM numbers")).isFullyPushedDown().matches("VALUES BIGINT '7'");
		assertThat(query("SELECT style, count(*) FROM numbers GROUP BY style")).isFullyPushedDown()
				.matches("VALUES (VARCHAR 'Wheat', BIGINT '2'), (VARCHAR 'Ale', BIGINT '1'), (VARCHAR 'None', BIGINT '1'), "
						+ "(VARCHAR 'Extreme', BIGINT '3')");
	}
}
