package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * Aggregations return the same results whether Redis or Trino computes them.
 */
public class TestAggregationPushdown extends AbstractTestQueryFramework {

	protected Deployment deployment() {
		return Deployment.NON_SHARDED;
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		RediSearchServer redisearch = closeAfterClass(new RediSearchServer(deployment()));
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("beers", CreateArgs.builder().withPrefix("beer:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("brewery_id").build(),
						NumericFieldArgs.builder().name("abv").build()));
		redisearch.awaitIndexed("beers");
		// last_mod isn't indexed
		redis.hset("beer:1", Map.of("id", "1", "brewery_id", "812", "abv", "4.5", "last_mod", "2010-07-22"));
		redis.hset("beer:2", Map.of("id", "2", "brewery_id", "812", "abv", "5.0"));
		redis.hset("beer:3", Map.of("id", "3", "brewery_id", "264", "abv", "6.0"));
		redis.hset("beer:4", Map.of("id", "4", "brewery_id", "264", "abv", "6.0"));
		redis.hset("beer:5", Map.of("id", "5", "brewery_id", "264"));
		redis.hset("beer:6", Map.of("id", "6", "brewery_id", "100"));
		createNumbers(redisearch);
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch);
	}

	/**
	 * Doubles that Redis, which formats the numbers it computes to 12 significant digits, would round: two that only
	 * differ after 12 digits, a sum with more, and the largest, smallest normal and a negative one. The document
	 * without one has its own group: a sharded database's shards return nan as the sum of no values, which the
	 * coordinator then adds.
	 */
	static void createNumbers(RediSearchServer redisearch) {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("numbers", CreateArgs.builder().withPrefix("number:").build(),
				List.of(NumericFieldArgs.builder().name("d").build(), TagFieldArgs.builder().name("style").build()));
		redisearch.awaitIndexed("numbers");
		redis.hset("number:1", Map.of("d", "0.1234567890123456", "style", "Wheat"));
		redis.hset("number:2", Map.of("d", "229577310901.21", "style", "Wheat"));
		redis.hset("number:3", Map.of("d", "0.1234567890123457", "style", "Ale"));
		redis.hset("number:4", Map.of("style", "None"));
		redis.hset("number:5", Map.of("d", "-1.0000000000000002", "style", "Extreme"));
		redis.hset("number:6", Map.of("d", "2.2250738585072014E-308", "style", "Extreme"));
		redis.hset("number:7", Map.of("d", "1.7976931348623157E308", "style", "Extreme"));
	}

	@Test
	public void testExactDoubles() {
		assertExact("SELECT style, sum(d), min(d), max(d), avg(d) FROM numbers GROUP BY style", "VALUES "
				+ "(VARCHAR 'Wheat', DOUBLE '0.1234567890123456' + DOUBLE '229577310901.21', DOUBLE '0.1234567890123456', "
				+ "DOUBLE '229577310901.21', (DOUBLE '0.1234567890123456' + DOUBLE '229577310901.21') / 2), "
				+ "(VARCHAR 'Ale', DOUBLE '0.1234567890123457', DOUBLE '0.1234567890123457', DOUBLE '0.1234567890123457', "
				+ "DOUBLE '0.1234567890123457'), "
				+ "(VARCHAR 'None', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)), "
				// The sum, in any order, is the largest double
				+ "(VARCHAR 'Extreme', DOUBLE '1.7976931348623157E308', DOUBLE '-1.0000000000000002', "
				+ "DOUBLE '1.7976931348623157E308', DOUBLE '1.7976931348623157E308' / 3)");
		assertExact("SELECT sum(d), max(d) FROM numbers WHERE style = 'Wheat'",
				"VALUES (DOUBLE '0.1234567890123456' + DOUBLE '229577310901.21', DOUBLE '229577310901.21')");
		// Formatted to 12 digits, the first two keys would be the same
		assertExact("SELECT d, count(*) FROM numbers GROUP BY d", "VALUES (DOUBLE '0.1234567890123456', BIGINT '1'), "
				+ "(DOUBLE '0.1234567890123457', BIGINT '1'), (DOUBLE '229577310901.21', BIGINT '1'), "
				+ "(CAST(NULL AS DOUBLE), BIGINT '1'), (DOUBLE '-1.0000000000000002', BIGINT '1'), "
				+ "(DOUBLE '2.2250738585072014E-308', BIGINT '1'), (DOUBLE '1.7976931348623157E308', BIGINT '1')");
	}

	private void assertExact(String sql, String expected) {
		assertThat(query(sql)).isFullyPushedDown();
		RediSearchQueryRunner.assertExactRows(getQueryRunner(), sql, expected);
	}

	@Test
	public void testCountColumn() {
		assertThat(query("SELECT count(*) FROM beers")).isFullyPushedDown().matches("VALUES BIGINT '6'");
		// Redis counts documents, so Trino counts the non-null values
		assertThat(query("SELECT count(abv) FROM beers")).matches("VALUES BIGINT '4'");
		assertThat(query("SELECT count(last_mod) FROM beers")).matches("VALUES BIGINT '1'");
		assertThat(query("SELECT brewery_id, count(abv) FROM beers GROUP BY brewery_id"))
				.matches("VALUES (VARCHAR '812', BIGINT '2'), (VARCHAR '264', BIGINT '2'), (VARCHAR '100', BIGINT '0')");
		assertThat(query("SELECT brewery_id, count(*), count(abv) FROM beers GROUP BY brewery_id"))
				.matches("VALUES (VARCHAR '812', BIGINT '2', BIGINT '2'), (VARCHAR '264', BIGINT '3', BIGINT '2'), "
						+ "(VARCHAR '100', BIGINT '1', BIGINT '0')");
	}

	@Test
	public void testHaving() {
		// The aggregation is pushed down and Trino filters the groups
		assertThat(query("SELECT brewery_id, count(*) FROM beers GROUP BY brewery_id HAVING count(*) > 1"))
				.isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES (VARCHAR '812', BIGINT '2'), (VARCHAR '264', BIGINT '3')");
		assertThat(query("SELECT brewery_id, cnt FROM (SELECT brewery_id, count(*) cnt FROM beers GROUP BY brewery_id) "
				+ "WHERE cnt > 1")).matches("VALUES (VARCHAR '812', BIGINT '2'), (VARCHAR '264', BIGINT '3')");
		assertThat(query("SELECT brewery_id, sum(abv) FROM beers GROUP BY brewery_id HAVING sum(abv) > 10"))
				.matches("VALUES (VARCHAR '264', DOUBLE '12.0')");
		assertThat(query("SELECT count(*) FROM beers HAVING count(*) > 10")).returnsEmptyResult();
	}

	@Test
	public void testAggregationsTrinoComputes() {
		assertThat(query("SELECT count(DISTINCT abv), sum(DISTINCT abv), sum(abv) FROM beers"))
				.matches("VALUES (BIGINT '3', DOUBLE '15.5', DOUBLE '21.5')");
		assertThat(query("SELECT count(*) FILTER (WHERE abv > 5), max(abv) FILTER (WHERE abv < 5) FROM beers"))
				.matches("VALUES (BIGINT '2', DOUBLE '4.5')");
		assertThat(query("SELECT max(abv, 2) FROM beers")).matches("VALUES ARRAY[DOUBLE '6.0', DOUBLE '6.0']");
	}
}
