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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * Aggregations return the same results whether Redis or Trino computes them.
 */
public class TestAggregationPushdown extends AbstractTestQueryFramework {
	private RedisCommands<String, String> redis;

	protected Deployment deployment() {
		return Deployment.NON_SHARDED;
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		RediSearchServer redisearch = closeAfterClass(new RediSearchServer(deployment()));
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		this.redis = redis;
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
		createSplit(redisearch);
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch);
	}

	/**
	 * Groups whose documents with and without values are on the same shard of a sharded database, and on different
	 * ones. A shard's SUM and AVG of no values are nan, which the coordinator adds to the other shards' sums, and its
	 * AVG divides by the number of documents rather than of values. The sums are exact in any order.
	 */
	private static void createSplit(RediSearchServer redisearch) {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("split", CreateArgs.builder().withPrefix("split:").build(),
				List.of(TagFieldArgs.builder().name("g").build(), NumericFieldArgs.builder().name("d").build(),
						NumericFieldArgs.builder().name("e").build()));
		redisearch.awaitIndexed("split");
		List<String> keys = redisearch.keysOnShards("split:", 0, 1, 0, 0, 1, 1, 1, 0);
		// The only value of d on one shard, and a document without it on the other
		redis.hset(keys.get(0), Map.of("g", "Ale", "d", "0.1234567890123457", "e", "2"));
		redis.hset(keys.get(1), Map.of("g", "Ale"));
		// A document without d next to one with it, and one with d but not e on the other shard
		redis.hset(keys.get(2), Map.of("g", "Mixed", "d", "4.5", "e", "1"));
		redis.hset(keys.get(3), Map.of("g", "Mixed", "e", "3"));
		redis.hset(keys.get(4), Map.of("g", "Mixed", "d", "1.5"));
		// Both on one shard
		redis.hset(keys.get(5), Map.of("g", "Shared", "d", "4.5"));
		redis.hset(keys.get(6), Map.of("g", "Shared"));
		redis.hset(keys.get(7), Map.of("g", "None"));
	}

	@Test
	public void testIntegerWideningAggregates() {
		assertUpdate("CREATE TABLE widening (g varchar, s smallint, t tinyint, i integer)");
		try {
			assertUpdate("INSERT INTO widening VALUES ('a', 2, 1, 2147483647), ('a', 4, 3, -2147483648), "
					+ "('a', NULL, NULL, NULL), ('b', -2, -1, 7), ('b', NULL, 2, NULL), ('none', NULL, NULL, NULL)", 6);
			// Trino inserts integer widening casts into both SUM(s) and AVG(t), as in ClickBench Q3.
			assertExact("SELECT sum(s), count(*), avg(t) FROM widening", "VALUES (BIGINT '4', BIGINT '6', DOUBLE '1.25')");
			QueryRunner.MaterializedResultWithPlan mixed = getDistributedQueryRunner().executeWithPlan(getSession(),
					"SELECT sum(s), count(*), avg(t) FROM widening");
			Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(mixed.queryId())
					.getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
					.reduce(Metrics.EMPTY, Metrics::mergeWith);
			// All six documents were aggregated in Redis: Trino received one aggregate row and no exact hash reads.
			assertThat(((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal()).isEqualTo(1);
			assertThat(((LongCount) metrics.getMetrics().get("redis.exact-hash-reads")).getTotal()).isZero();
			assertExact("SELECT sum(CAST(i AS BIGINT)) FROM widening", "VALUES BIGINT '6'");
			assertExact("SELECT g, sum(s), count(*), avg(t) FROM widening GROUP BY g", "VALUES "
					+ "(VARCHAR 'a', BIGINT '6', BIGINT '3', DOUBLE '2'), "
					+ "(VARCHAR 'b', BIGINT '-2', BIGINT '2', DOUBLE '0.5'), "
					+ "(VARCHAR 'none', CAST(NULL AS BIGINT), BIGINT '1', CAST(NULL AS DOUBLE))");
			assertThat(query("SELECT CAST(s AS BIGINT) FROM widening"))
					.isFullyPushedDown().matches("VALUES BIGINT '2', BIGINT '4', CAST(NULL AS BIGINT), BIGINT '-2', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)");
			assertExact("SELECT sum(s), count(*), avg(t) FROM widening WHERE g = 'absent'",
					"VALUES (CAST(NULL AS BIGINT), BIGINT '0', CAST(NULL AS DOUBLE))");
		} finally {
			assertUpdate("DROP TABLE widening");
		}
	}

	@Test
	public void testSumsAndAveragesOfGroupsWithoutValues() {
		assertExact("SELECT g, sum(d), avg(d) FROM split GROUP BY g", "VALUES "
				+ "(VARCHAR 'Ale', DOUBLE '0.1234567890123457', DOUBLE '0.1234567890123457'), "
				+ "(VARCHAR 'Mixed', DOUBLE '6', DOUBLE '3'), (VARCHAR 'Shared', DOUBLE '4.5', DOUBLE '4.5'), "
				+ "(VARCHAR 'None', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
		// Two averages, whose counts a sharded database's coordinator gave the same name
		assertExact("SELECT g, count(*), sum(d), avg(d), avg(e), min(d), max(d) FROM split GROUP BY g", "VALUES "
				+ "(VARCHAR 'Ale', BIGINT '2', DOUBLE '0.1234567890123457', DOUBLE '0.1234567890123457', DOUBLE '2', "
				+ "DOUBLE '0.1234567890123457', DOUBLE '0.1234567890123457'), "
				+ "(VARCHAR 'Mixed', BIGINT '3', DOUBLE '6', DOUBLE '3', DOUBLE '2', DOUBLE '1.5', DOUBLE '4.5'), "
				+ "(VARCHAR 'Shared', BIGINT '2', DOUBLE '4.5', DOUBLE '4.5', CAST(NULL AS DOUBLE), DOUBLE '4.5', "
				+ "DOUBLE '4.5'), "
				+ "(VARCHAR 'None', BIGINT '1', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), "
				+ "CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
		assertExact("SELECT count(*), sum(e), avg(e) FROM split", "VALUES (BIGINT '8', DOUBLE '6', DOUBLE '2')");
		assertExact("SELECT count(*), avg(d), avg(e) FROM split WHERE g = 'Mixed'",
				"VALUES (BIGINT '3', DOUBLE '3', DOUBLE '2')");
		assertExact("SELECT sum(d), avg(d) FROM split WHERE g = 'Shared'", "VALUES (DOUBLE '4.5', DOUBLE '4.5')");
		assertExact("SELECT sum(e), avg(e) FROM split WHERE g = 'Shared'",
				"VALUES (CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
	}

	@Test
	public void testAggregatesOfArithmetic() {
		// An APPLY step computes the arithmetic, and a document missing either value has none
		assertExact("SELECT g, sum(d * e), avg(d + e) FROM split GROUP BY g", "VALUES "
				+ "(VARCHAR 'Ale', DOUBLE '0.1234567890123457' * 2, DOUBLE '0.1234567890123457' + 2), "
				+ "(VARCHAR 'Mixed', DOUBLE '4.5', DOUBLE '5.5'), "
				+ "(VARCHAR 'Shared', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)), "
				+ "(VARCHAR 'None', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");
		assertExact("SELECT sum(-d * e), count(*) FROM split WHERE g = 'Mixed'", "VALUES (DOUBLE '-4.5', BIGINT '3')");
		// Constants Java writes in scientific notation, 1.0E-4 and 1.0E10, which APPLY reads as numbers
		assertExact("SELECT sum(d * 1e-4), avg(d * 1e10) FROM split WHERE g = 'Mixed'",
				"VALUES (DOUBLE '4.5' * 1e-4 + DOUBLE '1.5' * 1e-4, (DOUBLE '4.5' * 1e10 + DOUBLE '1.5' * 1e10) / 2)");
		// Scans compute it in the connector
		assertThat(query("SELECT g, d * e, d / 0 FROM split WHERE g IN ('Ale', 'Mixed')")).isFullyPushedDown()
				.matches("VALUES (VARCHAR 'Ale', DOUBLE '0.1234567890123457' * 2, infinity()), "
						+ "(VARCHAR 'Ale', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)), "
						+ "(VARCHAR 'Mixed', DOUBLE '4.5', infinity()), "
						+ "(VARCHAR 'Mixed', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)), "
						+ "(VARCHAR 'Mixed', CAST(NULL AS DOUBLE), infinity())");
		// Trino orders nan as larger than other doubles, where Redis's MAX doesn't
		assertThat(query("SELECT max(d * e) FROM split")).isNotFullyPushedDown(AggregationNode.class)
				.matches("VALUES DOUBLE '4.5'");
	}

	/**
	 * Doubles that Redis, which formats the numbers it computes to 12 significant digits, would round: two that only
	 * differ after 12 digits, a sum with more, and the largest, smallest normal and a negative one. One document has
	 * none, in a group of its own.
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
		// A SUM of existence indicators counts present HASH fields, including unindexed ones.
		assertThat(query("SELECT count(abv) FROM beers")).isFullyPushedDown().matches("VALUES BIGINT '4'");
		assertThat(query("SELECT count(last_mod) FROM beers")).isFullyPushedDown().matches("VALUES BIGINT '1'");
		assertThat(query("SELECT brewery_id, count(abv) FROM beers GROUP BY brewery_id"))
				.isFullyPushedDown()
				.matches("VALUES (VARCHAR '812', BIGINT '2'), (VARCHAR '264', BIGINT '2'), (VARCHAR '100', BIGINT '0')");
		assertThat(query("SELECT brewery_id, count(*), count(abv) FROM beers GROUP BY brewery_id"))
				.isFullyPushedDown()
				.matches("VALUES (VARCHAR '812', BIGINT '2', BIGINT '2'), (VARCHAR '264', BIGINT '3', BIGINT '2'), "
						+ "(VARCHAR '100', BIGINT '1', BIGINT '0')");
		assertThat(query("SELECT count(abv), count(last_mod) FROM beers WHERE id = 'absent'"))
				.isFullyPushedDown().matches("VALUES (BIGINT '0', BIGINT '0')");
		assertThat(query("SELECT g, count(d), count(e) FROM split GROUP BY g")).isFullyPushedDown()
				.matches("VALUES (VARCHAR 'Ale', BIGINT '1', BIGINT '1'), "
						+ "(VARCHAR 'Mixed', BIGINT '2', BIGINT '2'), (VARCHAR 'Shared', BIGINT '1', BIGINT '0'), "
						+ "(VARCHAR 'None', BIGINT '0', BIGINT '0')");
	}

	@Test
	public void testBooleanDomainsMatchTheStoredValueDecoder() {
		assertUpdate("CREATE TABLE boolean_domains (id varchar, flag boolean)");
		try {
			redis.hset("boolean_domains:1", Map.of("id", "1", "flag", "TRUE"));
			redis.hset("boolean_domains:2", Map.of("id", "2", "flag", "false"));
			redis.hset("boolean_domains:3", Map.of("id", "3", "flag", "junk"));
			redis.hset("boolean_domains:4", Map.of("id", "4"));
			redis.hset("boolean_domains:5", Map.of("id", "5", "flag", "true"));
			assertThat(query("SELECT id FROM boolean_domains WHERE flag")).isFullyPushedDown()
					.matches("VALUES VARCHAR '1', '5'");
			assertThat(query("SELECT id FROM boolean_domains WHERE NOT flag")).isFullyPushedDown()
					.matches("VALUES VARCHAR '2', '3'");
			assertThat(query("SELECT id FROM boolean_domains WHERE flag = false OR flag IS NULL")).isFullyPushedDown()
					.matches("VALUES VARCHAR '2', '3', '4'");
			assertThat(query("SELECT count_if(flag) FROM boolean_domains WHERE flag")).isFullyPushedDown()
					.matches("VALUES BIGINT '2'");
			assertThat(query("SELECT b.id FROM boolean_domains b JOIN (VALUES true) t(flag) ON b.flag = t.flag"))
					.matches("VALUES VARCHAR '1', '5'");
		} finally {
			assertUpdate("DROP TABLE boolean_domains");
		}
	}

	@Test
	public void testGroupingPreservesDecodedSqlEquality() {
		assertUpdate("CREATE TABLE canonical_groups (id varchar, b boolean, u uuid, c char(2), r real, day date)");
		try {
			redis.hset("canonical_groups:1", Map.of("id", "1", "b", "TRUE", "u", "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
					"c", "a", "r", "1.000000001", "day", "2024-01-02Z"));
			redis.hset("canonical_groups:2", Map.of("id", "2", "b", "true", "u", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
					"c", "a ", "r", "1.000000002", "day", "2024-01-02+01:00"));
			for (String column : List.of("u", "r", "day")) {
				assertThat(query("SELECT count(DISTINCT " + column + ") FROM canonical_groups"))
						.isNotFullyPushedDown(AggregationNode.class).matches("VALUES BIGINT '1'");
			}
			assertThat(query("SELECT count(DISTINCT b) FROM canonical_groups")).isFullyPushedDown().matches("VALUES BIGINT '1'");
			assertThat(query("SELECT count(DISTINCT c) FROM canonical_groups")).isFullyPushedDown().matches("VALUES BIGINT '1'");
			assertThat(query("SELECT b, count(*) FROM canonical_groups GROUP BY b")).isFullyPushedDown()
					.matches("VALUES (true, BIGINT '2')");
			assertThat(query("SELECT count_if(b) FROM canonical_groups")).isFullyPushedDown().matches("VALUES BIGINT '2'");
			assertThat(query("SELECT r, count(*) FROM canonical_groups GROUP BY r")).isNotFullyPushedDown(AggregationNode.class)
					.matches("VALUES (REAL '1', BIGINT '2')");
		} finally {
			assertUpdate("DROP TABLE canonical_groups");
		}
	}

	@Test
	public void testDoubleGroupingTreatsSignedZeroAsOneSqlValue() {
		assertUpdate("CREATE TABLE signed_zero_groups (id varchar, d double)");
		try {
			assertUpdate("INSERT INTO signed_zero_groups VALUES ('a', -0e0), ('b', 0e0)", 2);
			assertThat(query("SELECT d, count(*) FROM signed_zero_groups GROUP BY d"))
					.matches("VALUES (DOUBLE '0', BIGINT '2')");
			assertThat(query("SELECT count(DISTINCT d) FROM signed_zero_groups")).matches("VALUES BIGINT '1'");
		} finally {
			assertUpdate("DROP TABLE signed_zero_groups");
		}
	}

	@Test
	public void testExactDistinctKeysAreGroupedBeforeTransfer() {
		assertThat(query("SELECT DISTINCT brewery_id FROM beers")).isFullyPushedDown()
				.matches("VALUES VARCHAR '812', '264', '100'");
		// Both grouping stages execute in one Redis request, returning only the final count.
		QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(),
				"SELECT count(DISTINCT brewery_id) FROM beers");
		assertThat(result.result().getOnlyValue()).isEqualTo(3L);
		Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
				.getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
				.reduce(Metrics.EMPTY, Metrics::mergeWith);
		assertThat(((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal()).isEqualTo(1);
		assertThat(query("SELECT DISTINCT last_mod FROM beers")).isFullyPushedDown()
				.matches("VALUES VARCHAR '2010-07-22', CAST(NULL AS VARCHAR)");
		assertThat(query("SELECT count(DISTINCT last_mod) FROM beers")).matches("VALUES BIGINT '1'");
		assertUpdate("CREATE TABLE distinct_alias_collision (__trino_distinct_rows varchar)");
		try {
			assertUpdate("INSERT INTO distinct_alias_collision VALUES ('same'), ('same')", 2);
			assertThat(query("SELECT DISTINCT __trino_distinct_rows FROM distinct_alias_collision"))
					.isFullyPushedDown().matches("VALUES VARCHAR 'same'");
		} finally {
			assertUpdate("DROP TABLE distinct_alias_collision");
		}
	}

	@Test
	public void testExactHashTopNForLargeIntegersAndStrings() {
		assertUpdate("CREATE TABLE exact_ranking (id varchar, value bigint, label varchar)");
		try {
			assertUpdate("INSERT INTO exact_ranking VALUES ('a', 9007199254740992, '😀'), "
					+ "('b', 9007199254740993, ''), ('c', -9223372036854775808, ''), "
					+ "('d', 9223372036854775807, 'x'), ('e', NULL, NULL)", 5);
			assertQueryOrdered("SELECT id, value FROM exact_ranking ORDER BY value DESC LIMIT 3",
					"VALUES ('d', CAST('9223372036854775807' AS BIGINT)), ('b', CAST('9007199254740993' AS BIGINT)), ('a', CAST('9007199254740992' AS BIGINT))");
			assertQueryOrdered("SELECT id FROM exact_ranking ORDER BY label LIMIT 5",
					"VALUES ('c'), ('d'), ('b'), ('a'), ('e')");
			assertQueryOrdered("SELECT id FROM exact_ranking ORDER BY label DESC LIMIT 3",
					"VALUES ('a'), ('b'), ('d')");
		} finally {
			assertUpdate("DROP TABLE exact_ranking");
		}
	}

	@Test
	public void testStoredBooleanAggregateFilters() {
		assertUpdate("CREATE TABLE filtered_values (g varchar, x double, enabled boolean)");
		try {
			assertUpdate("INSERT INTO filtered_values VALUES ('a', 2, true), ('a', NULL, true), ('a', 10, false), "
					+ "('a', 20, NULL), ('b', 7, false), ('b', NULL, true), ('c', 4, NULL)", 7);
			assertThat(query("SELECT count(*) FILTER (WHERE enabled), count(x) FILTER (WHERE enabled), "
					+ "sum(x) FILTER (WHERE enabled), avg(x) FILTER (WHERE enabled) FROM filtered_values"))
					.isNotFullyPushedDown(AggregationNode.class).matches("VALUES (BIGINT '3', BIGINT '1', DOUBLE '2', DOUBLE '2')");
			assertThat(query("SELECT g, count(*) FILTER (WHERE enabled), count(x) FILTER (WHERE enabled), "
					+ "sum(x) FILTER (WHERE enabled) FROM filtered_values GROUP BY g"))
					.isNotFullyPushedDown(AggregationNode.class).matches("VALUES (VARCHAR 'a', BIGINT '2', BIGINT '1', DOUBLE '2'), "
							+ "(VARCHAR 'b', BIGINT '1', BIGINT '0', CAST(NULL AS DOUBLE)), "
							+ "(VARCHAR 'c', BIGINT '0', BIGINT '0', CAST(NULL AS DOUBLE))");
			assertThat(query("SELECT count_if(enabled) FROM filtered_values")).isFullyPushedDown()
					.matches("VALUES BIGINT '3'");
			assertThat(query("SELECT g, count_if(enabled) FROM filtered_values GROUP BY g")).isFullyPushedDown()
					.matches("VALUES (VARCHAR 'a', BIGINT '2'), (VARCHAR 'b', BIGINT '1'), (VARCHAR 'c', BIGINT '0')");
		} finally {
			assertUpdate("DROP TABLE filtered_values");
		}
	}

	@Test
	public void testAggregateInternalAliasesDoNotShadowSourceFields() {
		// External indexes can retain mixed-case physical names even though Trino normalizes SQL column names.
		redis.ftCreate("aggregate_alias_collision", CreateArgs.builder().withPrefix("aggregate_alias_collision:").skipInitialScan().build(), List.of(
				TagFieldArgs.builder().name("syntheticColumn0").build(), NumericFieldArgs.builder().name("n").build(),
				NumericFieldArgs.builder().name("__has_syntheticColumn0").build(), NumericFieldArgs.builder().name("__value_syntheticColumn0").build(),
				NumericFieldArgs.builder().name("__count_syntheticColumn0").build(), NumericFieldArgs.builder().name("__precision_count_syntheticColumn0").build()));
		try {
			assertUpdate("INSERT INTO aggregate_alias_collision VALUES ('a', 2, 3, 5, 7, 11), ('a', 4, 13, 17, 19, 23), "
					+ "('b', NULL, 29, 31, 37, 41)", 3);
			assertThat(query("SELECT syntheticColumn0, sum(n), sum(__has_syntheticColumn0), sum(__value_syntheticColumn0), "
					+ "count(n), sum(__count_syntheticColumn0), sum(__precision_count_syntheticColumn0) "
					+ "FROM aggregate_alias_collision GROUP BY syntheticColumn0"))
					.isFullyPushedDown().matches("VALUES (VARCHAR 'a', DOUBLE '6', DOUBLE '16', DOUBLE '22', BIGINT '2', DOUBLE '26', DOUBLE '34'), "
							+ "(VARCHAR 'b', CAST(NULL AS DOUBLE), DOUBLE '29', DOUBLE '31', BIGINT '0', DOUBLE '37', DOUBLE '41')");
		} finally {
			assertUpdate("DROP TABLE aggregate_alias_collision");
		}
	}

	@Test
	public void testHaving() {
		assertThat(query("SELECT brewery_id, sum(abv) FROM beers GROUP BY brewery_id HAVING sum(abv) IS NULL"))
				.isNotFullyPushedDown(FilterNode.class)
				.matches("VALUES (VARCHAR '100', CAST(NULL AS DOUBLE))");
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
