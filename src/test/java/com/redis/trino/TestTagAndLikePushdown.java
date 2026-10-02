package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * TAG and wildcard queries match differently from SQL's {@code =} and {@code LIKE}. Redis keeps the equal rows with a
 * FILTER on TAG equality, and Trino evaluates {@code LIKE} and the values a tag query can't match.
 */
public class TestTagAndLikePushdown extends AbstractTestQueryFramework {

	// More distinct matching values than Redis expands a wildcard to by default (MAXEXPANSIONS 200)
	private static final int MANY = 300;

	private RediSearchServer redisearch;

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = closeAfterClass(new RediSearchServer());
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("beers", CreateArgs.builder().withPrefix("beer:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("style").build(),
						TagFieldArgs.builder().name("code").caseSensitive().build(),
						TagFieldArgs.builder().name("tags").separator(";").build(),
						TextFieldArgs.builder().name("name").build()));
		redisearch.awaitIndexed("beers");
		redis.hset("beer:1", Map.of("id", "1", "style", "Wheat", "code", "Wheat", "tags", "a,b", "name", "Hocus Pocus"));
		redis.hset("beer:2", Map.of("id", "2", "style", "wheat", "code", "wheat", "tags", "a;b", "name", "Big Hocus"));
		redis.hset("beer:3", Map.of("id", "3", "style", "a,b", "code", "a,b", "tags", "a", "name", "The Pocus Hocus"));
		redis.hset("beer:4", Map.of("id", "4", "style", "a", "code", "a", "name", "Pocus"));
		redis.hset("beer:5", Map.of("id", "5", "style", " a", "code", "a ", "name", "Into the"));
		redis.hset("beer:6", Map.of("id", "6", "style", "Café", "code", "Café", "name", "Café Hocus"));
		redis.ftCreate("many", CreateArgs.builder().withPrefix("many:").build(),
				List.of(TagFieldArgs.builder().name("tag").build(), TextFieldArgs.builder().name("name").build()));
		redisearch.awaitIndexed("many");
		for (int i = 0; i < MANY; i++) {
			redis.hset("many:" + i, Map.of("tag", "v" + i + "cus", "name", "pre" + i + "cus"));
		}
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch);
	}

	@Test
	public void testTagCase() {
		// Without CASESENSITIVE, @style:{wheat} also matches Wheat, which the FILTER drops
		assertThat(query("SELECT id FROM beers WHERE style = 'wheat'")).isFullyPushedDown()
				.matches("VALUES VARCHAR '2'");
		assertThat(explain("SELECT id FROM beers WHERE style = 'wheat'")).doesNotContain("constraint=ALL");
		assertThat(query("SELECT id FROM beers WHERE style = 'Wheat'")).matches("VALUES VARCHAR '1'");
		assertThat(query("SELECT id FROM beers WHERE style IN ('WHEAT', 'wheat')")).matches("VALUES VARCHAR '2'");
		assertThat(query("SELECT id FROM beers WHERE code = 'wheat'")).matches("VALUES VARCHAR '2'");
		// Aggregations over a TAG filter count the equal rows only
		assertThat(query("SELECT count(*) FROM beers WHERE style = 'wheat'")).isFullyPushedDown()
				.matches("VALUES BIGINT '1'");
	}

	@Test
	public void testTagSeparator() {
		// Redis splits a, b and a,b into tags on the separator, so @style:{a} matches all of them
		assertThat(query("SELECT id FROM beers WHERE style = 'a'")).isFullyPushedDown().matches("VALUES VARCHAR '4'");
		assertThat(query("SELECT id FROM beers WHERE code = 'a'")).matches("VALUES VARCHAR '4'");
		// and a query for a value containing it matches nothing, so Trino evaluates the filter alone
		assertThat(query("SELECT id FROM beers WHERE style = 'a,b'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '3'");
		assertThat(explain("SELECT id FROM beers WHERE style = 'a,b'")).contains("constraint=ALL");
		assertThat(query("SELECT id FROM beers WHERE style IN ('a', 'a,b')")).matches("VALUES VARCHAR '3', '4'");
		// The separator is the field's own
		assertThat(query("SELECT id FROM beers WHERE tags = 'a,b'")).matches("VALUES VARCHAR '1'");
		assertThat(explain("SELECT id FROM beers WHERE tags = 'a,b'")).doesNotContain("constraint=ALL");
		assertThat(query("SELECT id FROM beers WHERE tags = 'a;b'")).matches("VALUES VARCHAR '2'");
		assertThat(explain("SELECT id FROM beers WHERE tags = 'a;b'")).contains("constraint=ALL");
		assertThat(query("SELECT id FROM beers WHERE tags = 'a'")).matches("VALUES VARCHAR '3'");
	}

	@Test
	public void testTagWhitespaceAndNonAscii() {
		// Redis trims whitespace around tags, so @code:{a} matches 'a ', and a query for ' a' matches nothing
		assertThat(query("SELECT id FROM beers WHERE style = ' a'")).matches("VALUES VARCHAR '5'");
		assertThat(query("SELECT id FROM beers WHERE code = 'a '")).matches("VALUES VARCHAR '5'");
		assertThat(explain("SELECT id FROM beers WHERE code = 'a '")).contains("constraint=ALL");
		assertThat(query("SELECT id FROM beers WHERE style = 'Café'")).matches("VALUES VARCHAR '6'");
		assertThat(query("SELECT id FROM beers WHERE code IN ('Café', 'Wheat')")).matches("VALUES VARCHAR '1', '6'");
		// An empty tag isn't a valid query, so Trino evaluates it
		assertThat(query("SELECT id FROM beers WHERE style = ''")).returnsEmptyResult();
	}

	@Test
	public void testLike() {
		// A wildcard matches any term of a TEXT value, ignoring case
		assertThat(query("SELECT id FROM beers WHERE name LIKE '%Pocus'")).isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES VARCHAR '1', '4'");
		assertThat(query("SELECT id FROM beers WHERE name LIKE '%pocus'")).returnsEmptyResult();
		// Stop words aren't indexed
		assertThat(query("SELECT id FROM beers WHERE name LIKE '%the'")).matches("VALUES VARCHAR '5'");
		// TAG wildcards ignore case without CASESENSITIVE
		assertThat(query("SELECT id FROM beers WHERE style LIKE '%HEAT'")).returnsEmptyResult();
		// Redis has no single-character wildcard outside w'...', no infix shorter than two characters, and rejects a%c
		assertThat(query("SELECT id FROM beers WHERE code LIKE '_heat'")).matches("VALUES VARCHAR '1', '2'");
		assertThat(query("SELECT id FROM beers WHERE code LIKE '%b%'")).matches("VALUES VARCHAR '3'");
		assertThat(query("SELECT id FROM beers WHERE style LIKE 'a%b'")).matches("VALUES VARCHAR '3'");
	}

	@Test
	public void testLikeMatchesMoreValuesThanRedisExpands() {
		assertThat(query("SELECT count(*) FROM many WHERE tag LIKE '%cus'")).matches("VALUES BIGINT '" + MANY + "'");
		assertThat(query("SELECT count(*) FROM many WHERE name LIKE '%cus'")).matches("VALUES BIGINT '" + MANY + "'");
	}

	@Test
	public void testCreatedTagFields() {
		// CREATE TABLE and ADD COLUMN make TAG fields that are CASESENSITIVE and don't split values on ','
		assertUpdate("CREATE TABLE drinks (id varchar, style varchar)");
		assertUpdate("INSERT INTO drinks VALUES ('1', 'Wheat'), ('2', 'wheat'), ('3', 'a,b'), ('4', 'a')", 4);
		assertUpdate("ALTER TABLE drinks ADD COLUMN code varchar");
		redisearch.awaitIndexed("drinks");
		assertUpdate("INSERT INTO drinks VALUES ('5', 'x', 'a,b')", 1);
		assertThat(searchIds("drinks", "@style:{wheat}")).containsExactly("2");
		assertThat(searchIds("drinks", "@style:{a}")).containsExactly("4");
		assertThat(searchIds("drinks", "@style:{a\\,b}")).containsExactly("3");
		assertThat(searchIds("drinks", "@code:{a\\,b}")).containsExactly("5");
		// So Redis can prefilter values with commas
		assertThat(query("SELECT id FROM drinks WHERE style = 'a,b'")).matches("VALUES VARCHAR '3'");
		assertThat(explain("SELECT id FROM drinks WHERE style = 'a,b'")).doesNotContain("constraint=ALL");
		assertThat(query("SELECT id FROM drinks WHERE code IN ('a,b', 'x')")).matches("VALUES VARCHAR '5'");
		assertThat(explain("SELECT id FROM drinks WHERE code IN ('a,b', 'x')")).doesNotContain("constraint=ALL");
	}

	private List<String> searchIds(String index, String query) {
		return redisearch.getConnection().sync().ftSearch(index, query).getResults().stream()
				.map(result -> result.getFields().get("id").asString()).sorted().toList();
	}

	private String explain(String sql) {
		return (String) computeActual("EXPLAIN " + sql).getOnlyValue();
	}
}
