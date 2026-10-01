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
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * Filters return the same rows whether Redis or Trino evaluates them.
 */
public class TestFilterPushdown extends AbstractTestQueryFramework {

	protected Deployment deployment() {
		return Deployment.NON_SHARDED;
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		RediSearchServer redisearch = closeAfterClass(new RediSearchServer(deployment()));
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("beers", CreateArgs.builder().withPrefix("beer:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("style").build(),
						TextFieldArgs.builder().name("name").build(), NumericFieldArgs.builder().name("abv").build()));
		redisearch.awaitIndexed("beers");
		redis.hset("beer:1", Map.of("id", "1", "style", "Wheat", "name", "Hocus Pocus", "abv", "4.5"));
		redis.hset("beer:2", Map.of("id", "2", "style", "Witbier", "name", "Grimm's Witbier", "abv", "5.0"));
		redis.hset("beer:3", Map.of("id", "3", "style", "Brown Ale", "name", "Beer Town Brown", "abv", "6.0"));
		redis.hset("beer:4", Map.of("id", "4", "name", "Pocus", "abv", "7.2"));
		redis.hset("beer:5", Map.of("id", "5", "style", "Wheat", "name", "The Pocus Hocus"));
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch);
	}

	@Test
	public void testIsNull() {
		// Redis can't match a missing field, so Trino evaluates these
		assertThat(query("SELECT id FROM beers WHERE abv IS NULL")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '5'");
		assertThat(query("SELECT id FROM beers WHERE abv IS NOT NULL")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '1', '2', '3', '4'");
		assertThat(query("SELECT id FROM beers WHERE abv > 5 OR abv IS NULL")).matches("VALUES VARCHAR '3', '4', '5'");
		assertThat(query("SELECT id FROM beers WHERE style IS NULL")).matches("VALUES VARCHAR '4'");
		assertThat(query("SELECT id FROM beers WHERE style = 'Wheat' OR style IS NULL"))
				.matches("VALUES VARCHAR '1', '4', '5'");
	}

	@Test
	public void testVarcharRanges() {
		assertThat(query("SELECT id FROM beers WHERE id <> '1'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '2', '3', '4', '5'");
		assertThat(query("SELECT id FROM beers WHERE id BETWEEN '2' AND '3'")).matches("VALUES VARCHAR '2', '3'");
		assertThat(query("SELECT id FROM beers WHERE name > 'H'")).matches("VALUES VARCHAR '1', '4', '5'");
		assertThat(query("SELECT id FROM beers WHERE name <> 'Pocus'")).matches("VALUES VARCHAR '1', '2', '3', '5'");
	}

	@Test
	public void testTextEquality() {
		// Redis returns the documents containing the words, and Trino keeps the equal ones
		assertThat(query("SELECT id FROM beers WHERE name = 'Pocus'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '4'");
		assertThat(query("SELECT id FROM beers WHERE name = 'pocus hocus'")).returnsEmptyResult();
		assertThat(query("SELECT id FROM beers WHERE name IN ('Pocus', 'Town')")).matches("VALUES VARCHAR '4'");
		assertThat(query("SELECT id FROM beers WHERE name = 'Grimm''s Witbier'")).matches("VALUES VARCHAR '2'");
		assertThat(query("SELECT id FROM beers WHERE name = 'The Pocus Hocus'")).matches("VALUES VARCHAR '5'");
		assertThat(explain("SELECT id FROM beers WHERE name = 'Pocus'")).doesNotContain("constraint=ALL");
		// A value with only stop words has no terms for Redis to match, so the whole filter stays with Trino
		assertThat(query("SELECT id FROM beers WHERE name IN ('The', 'Pocus')")).matches("VALUES VARCHAR '4'");
		assertThat(explain("SELECT id FROM beers WHERE name IN ('The', 'Pocus')"))
				.contains("constraint=ALL");
	}

	@Test
	public void testExactFiltersPushedDown() {
		assertThat(query("SELECT id FROM beers WHERE abv > 5")).isFullyPushedDown().matches("VALUES VARCHAR '3', '4'");
		assertThat(query("SELECT id FROM beers WHERE abv BETWEEN 4 AND 5")).isFullyPushedDown()
				.matches("VALUES VARCHAR '1', '2'");
	}

	private String explain(String sql) {
		return (String) computeActual("EXPLAIN " + sql).getOnlyValue();
	}
}
