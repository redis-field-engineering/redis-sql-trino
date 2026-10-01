package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

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

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		RediSearchServer redisearch = closeAfterClass(new RediSearchServer());
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
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch);
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
