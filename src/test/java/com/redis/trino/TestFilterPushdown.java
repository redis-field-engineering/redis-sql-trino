package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.CreateArgs.TargetType;
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
		// SORTABLE fields, whose sort values are lowercased, and a field indexed under another name
		redis.ftCreate("styles", CreateArgs.builder().withPrefix("style:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("style").sortable().build(),
						TextFieldArgs.builder().name("name").sortable().build(), TagFieldArgs.builder().name("q").build(),
						TagFieldArgs.builder().name("brewery_name").as("brewery").build(),
						NumericFieldArgs.builder().name("abv").build()));
		redisearch.awaitIndexed("styles");
		redis.hset("style:1", Map.of("id", "1", "style", "Wheat", "name", "Hocus Pocus", "q", "say \"hi\"",
				"brewery_name", "Big Brew", "abv", "4.5"));
		redis.hset("style:2", Map.of("id", "2", "style", "wheat", "name", "hocus pocus", "q", "back\\slash",
				"brewery_name", "big brew", "abv", "5.0"));
		redis.hset("style:3", Map.of("id", "3", "style", "Wheat", "name", "Hocus Pocus Big", "q", "a\tb",
				"brewery_name", "Big Brew", "abv", "6.0"));
		redis.hset("style:4", Map.of("id", "4", "abv", "7.0"));
		redis.ftCreate("jsonstyles", CreateArgs.builder().on(TargetType.JSON).withPrefix("jsonstyle:").build(),
				List.of(TagFieldArgs.builder().name("$.id").as("id").build(),
						TagFieldArgs.builder().name("$.style").as("style").build(),
						TextFieldArgs.builder().name("$.name").as("name").build(),
						TagFieldArgs.builder().name("$.tags[*]").as("tags").build(),
						TagFieldArgs.builder().name("$.flag").as("flag").build()));
		redisearch.awaitIndexed("jsonstyles");
		redis.jsonSet("jsonstyle:1", JsonPath.ROOT_PATH, "{\"id\": \"1\", \"style\": \"Wheat\", \"name\": \"Hocus Pocus\", "
				+ "\"tags\": [\"a\", \"b\"], \"flag\": true}");
		redis.jsonSet("jsonstyle:2", JsonPath.ROOT_PATH, "{\"id\": \"2\", \"style\": \"wheat\", \"name\": \"hocus pocus\", "
				+ "\"tags\": [\"b\", \"a\"], \"flag\": false}");
		// An array at a path of single values, which the connector reads as its JSON text, and nulls
		redis.jsonSet("jsonstyle:3", JsonPath.ROOT_PATH, "{\"id\": \"3\", \"style\": [\"Wheat\", \"Ale\"], "
				+ "\"name\": null, \"tags\": [], \"flag\": null}");
		redis.jsonSet("jsonstyle:4", JsonPath.ROOT_PATH, "{\"id\": \"4\"}");
		redis.jsonSet("jsonstyle:5", JsonPath.ROOT_PATH, "{\"id\": \"5\", \"style\": \"1\", \"flag\": \"1\"}");
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
		// Redis returns the documents containing the words, and a FILTER keeps the equal ones
		assertThat(query("SELECT id FROM beers WHERE name = 'Pocus'")).isFullyPushedDown()
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

	@Test
	public void testEqualityPushedDownWithAggregationsAndLimit() {
		// A FILTER checks TAG and TEXT equality in Redis, so it computes aggregations and LIMIT too
		assertThat(query("SELECT count(*), sum(abv) FROM styles WHERE style = 'Wheat'")).isFullyPushedDown()
				.matches("VALUES (BIGINT '2', DOUBLE '10.5')");
		assertThat(query("SELECT style, count(*) FROM styles WHERE style IN ('Wheat', 'wheat') GROUP BY style"))
				.isFullyPushedDown().matches("VALUES (VARCHAR 'Wheat', BIGINT '2'), (VARCHAR 'wheat', BIGINT '1')");
		assertThat(query("SELECT count(*) FROM styles WHERE name = 'Hocus Pocus'")).isFullyPushedDown()
				.matches("VALUES BIGINT '1'");
		assertThat(query("SELECT id FROM styles WHERE name IN ('Hocus Pocus', 'hocus pocus')")).isFullyPushedDown()
				.matches("VALUES VARCHAR '1', '2'");
		// @style:{Wheat} matches documents 1, 2 and 3 in that order: LIMIT counts the rows FILTER keeps
		assertThat(query("SELECT id FROM styles WHERE style = 'Wheat' LIMIT 2")).isFullyPushedDown()
				.matches("VALUES VARCHAR '1', '3'");
		assertThat(query("SELECT count(*) FROM styles WHERE brewery = 'Big Brew'")).isFullyPushedDown()
				.matches("VALUES BIGINT '2'");
	}

	@Test
	public void testFilterRejectsEveryMatch() {
		// @style:{WHEAT} ignores case, and the FILTER keeps none of the documents it matches
		assertThat(query("SELECT count(*), sum(abv) FROM styles WHERE style = 'WHEAT'")).isFullyPushedDown()
				.matches("VALUES (BIGINT '0', CAST(NULL AS double))");
		assertThat(query("SELECT style, count(*) FROM styles WHERE style = 'WHEAT' GROUP BY style")).isFullyPushedDown()
				.returnsEmptyResult();
	}

	@Test
	public void testFilterValues() {
		assertThat(query("SELECT id FROM styles WHERE q = 'say \"hi\"'")).isFullyPushedDown().matches("VALUES VARCHAR '1'");
		assertThat(query("SELECT id FROM styles WHERE q = 'back\\slash'")).isFullyPushedDown()
				.matches("VALUES VARCHAR '2'");
		// A string literal can't hold a control character, so Trino filters the rows
		assertThat(query("SELECT id FROM styles WHERE q = U&'a\\0009b'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '3'");
		assertThat(explain("SELECT id FROM styles WHERE q = U&'a\\0009b'")).doesNotContain("constraint=ALL");
		// IN nests its comparisons, so a long list doesn't nest deeply in Redis
		String values = IntStream.range(0, 2000).mapToObj(i -> "'v" + i + "'").collect(Collectors.joining(", "));
		assertThat(query("SELECT count(*) FROM styles WHERE style IN ('Wheat', " + values + ")")).isFullyPushedDown()
				.matches("VALUES BIGINT '2'");
	}

	@Test
	public void testJsonSubstringLikeRetainsResidual() {
		assertThat(query("SELECT id FROM jsonstyles WHERE style LIKE '%\"%'"))
				.isNotFullyPushedDown(FilterNode.class).matches("VALUES VARCHAR '3'");
	}

	@Test
	public void testJsonEquality() {
		// Scans keep the equal rows themselves, since FILTER can't compare the arrays DIALECT 3 loads
		assertThat(query("SELECT id FROM jsonstyles WHERE style = 'Wheat'")).isFullyPushedDown()
				.matches("VALUES VARCHAR '1'");
		assertThat(query("SELECT id FROM jsonstyles WHERE name = 'Hocus Pocus'")).isFullyPushedDown()
				.matches("VALUES VARCHAR '1'");
		assertThat(query("SELECT id FROM jsonstyles WHERE style IN ('Wheat', 'wheat')")).isFullyPushedDown()
				.matches("VALUES VARCHAR '1', '2'");
		// The first of the values at a path
		assertThat(query("SELECT id FROM jsonstyles WHERE tags = 'b'")).isFullyPushedDown().matches("VALUES VARCHAR '2'");
		// With DIALECT 2, aggregations compare the same values with FILTER
		assertThat(query("SELECT count(*) FROM jsonstyles WHERE style = 'wheat'")).isFullyPushedDown()
				.matches("VALUES BIGINT '1'");
		assertThat(query("SELECT style, count(*) FROM jsonstyles WHERE tags = 'a' GROUP BY style")).isFullyPushedDown()
				.matches("VALUES (VARCHAR 'Wheat', BIGINT '1')");
		assertThat(query("SELECT count(*) FROM jsonstyles WHERE name = 'hocus pocus'")).isFullyPushedDown()
				.matches("VALUES BIGINT '1'");
		// Redis would limit the documents @tags:{b} matches, 1 and 2, before the scan keeps the equal ones
		assertThat(query("SELECT id FROM jsonstyles WHERE tags = 'b' LIMIT 1")).matches("VALUES VARCHAR '2'");
	}

	@Test
	public void testJsonBooleans() {
		// A TAG field indexes a JSON boolean as the tag true, but the connector reads it as 1, so Trino compares 1
		assertThat(query("SELECT id FROM jsonstyles WHERE flag = '1'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '1', '5'");
		assertThat(query("SELECT id FROM jsonstyles WHERE flag = '0'")).matches("VALUES VARCHAR '2'");
		// even for a field of strings
		assertThat(query("SELECT id FROM jsonstyles WHERE style = '1'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '5'");
		assertThat(query("SELECT id FROM jsonstyles WHERE flag = 'true'")).isFullyPushedDown().returnsEmptyResult();
		assertThat(query("SELECT count(*) FROM jsonstyles WHERE flag = 'true'")).isFullyPushedDown()
				.matches("VALUES BIGINT '0'");
	}

	private String explain(String sql) {
		return (String) computeActual("EXPLAIN " + sql).getOnlyValue();
	}
}
