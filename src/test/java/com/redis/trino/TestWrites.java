package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.CreateArgs.TargetType;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.spi.type.BigintType;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * Rows written through the connector can be read back.
 */
public class TestWrites extends AbstractTestQueryFramework {

	private RediSearchServer redisearch;

	protected Deployment deployment() {
		return Deployment.NON_SHARDED;
	}

	protected Map<String, String> writeProperties() {
		return Map.of();
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = closeAfterClass(new RediSearchServer(deployment()));
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("docs", CreateArgs.builder().on(TargetType.JSON).withPrefix("doc:").build(),
				List.of(TagFieldArgs.builder().name("$.id").as("id").build(),
						TextFieldArgs.builder().name("$.message").as("message").build(),
						NumericFieldArgs.builder().name("$.score").as("score").build()));
		redisearch.awaitIndexed("docs");
		redis.jsonSet("doc:1", JsonPath.ROOT_PATH, "{\"id\": \"1\", \"message\": \"json doc\", \"score\": 3}");
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, List.of(), Map.of(), writeProperties());
	}

	@Test
	public void testMergeInsertWithoutLastColumn() {
		assertUpdate("CREATE TABLE merge_target (a varchar, b varchar, c varchar, d varchar)");
		try {
			// A column list that leaves out the table's last column failed in planning
			assertUpdate("MERGE INTO merge_target t USING (VALUES ('x')) s(a) ON t.a = s.a "
					+ "WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)", 1);
			assertUpdate("MERGE INTO merge_target t USING (VALUES ('p', 'q', 'r')) s(a, b, c) ON t.a = s.a "
					+ "WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (s.a, s.b, s.c)", 1);
			assertUpdate("MERGE INTO merge_target t USING (VALUES ('m', 'o')) s(a, d) ON t.a = s.a "
					+ "WHEN NOT MATCHED THEN INSERT (a, d) VALUES (s.a, s.d)", 1);
			assertUpdate("MERGE INTO merge_target t USING (VALUES ('x', 'y')) s(a, d) ON t.a = s.a "
					+ "WHEN MATCHED THEN UPDATE SET d = s.d WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)", 1);
			assertThat(query("SELECT a, b, c, d FROM merge_target")).matches("VALUES "
					+ "(VARCHAR 'x', CAST(NULL AS varchar), CAST(NULL AS varchar), VARCHAR 'y'), "
					+ "(VARCHAR 'p', VARCHAR 'q', VARCHAR 'r', CAST(NULL AS varchar)), "
					+ "(VARCHAR 'm', CAST(NULL AS varchar), CAST(NULL AS varchar), VARCHAR 'o')");
		} finally {
			assertUpdate("DROP TABLE merge_target");
		}
	}

	@Test
	public void testCreateTableAsSelectTypes() {
		// BOOLEAN and DATE became NUMERIC fields that couldn't index the values written to them
		assertUpdate("CREATE TABLE ctas_types AS SELECT 'a' id, true flag, DATE '2024-01-02' day, "
				+ "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' uuid, TIMESTAMP '2024-01-02 03:04:05.006' ts, "
				+ "DECIMAL '1.25' price, BIGINT '7' quantity", 1);
		try {
			// Columns read back as the types they were created with, not only as the index's field types
			assertThat(query("SELECT id, flag, day, uuid, ts, price, quantity FROM ctas_types"))
					.matches("VALUES ('a', true, DATE '2024-01-02', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', "
							+ "TIMESTAMP '2024-01-02 03:04:05.006', DECIMAL '1.25', BIGINT '7')");
			assertThat(query("SELECT id FROM ctas_types WHERE flag AND day = DATE '2024-01-02'")).matches("VALUES 'a'");
		} finally {
			assertUpdate("DROP TABLE ctas_types");
		}
	}

	@Test
	public void testDeclaredColumnTypes() {
		assertUpdate("CREATE TABLE typed (b bigint, i integer, s smallint, t tinyint, d double, r real, "
				+ "dec decimal(10, 2), longdec decimal(30, 5), v varchar(10), c char(3), flag boolean, day date, "
				+ "ts timestamp(3), tstz timestamp(3) with time zone, u uuid)");
		try {
			assertThat(query("SELECT column_name, data_type FROM information_schema.columns "
					+ "WHERE table_schema = 'tpch' AND table_name = 'typed'")).skippingTypesCheck()
					.matches("VALUES ('b', 'bigint'), ('i', 'integer'), ('s', 'smallint'), ('t', 'tinyint'), "
							+ "('d', 'double'), ('r', 'real'), ('dec', 'decimal(10,2)'), ('longdec', 'decimal(30,5)'), "
							+ "('v', 'varchar(10)'), ('c', 'char(3)'), ('flag', 'boolean'), ('day', 'date'), "
							+ "('ts', 'timestamp(3)'), ('tstz', 'timestamp(3) with time zone'), ('u', 'uuid')");
			// Literals of the declared types, which INSERT rejected when the columns read back as DOUBLE and VARCHAR
			// More digits than a double holds, or than Redis returns for NUMERIC fields loaded by name
			assertUpdate("INSERT INTO typed VALUES (BIGINT '9007199254740993', 2147483647, SMALLINT '-32768', "
					+ "TINYINT '127', 0.1234567890123456E0, REAL '2.5', DECIMAL '12345678.91', "
					+ "DECIMAL '1234567890123456789012345.12345', "
					+ "'abc', 'ab', true, DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.006', "
					+ "TIMESTAMP '2024-01-02 03:04:05.006 UTC', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')", 1);
			assertUpdate("INSERT INTO typed (b, i, s, t, r, dec) VALUES (1, 1, 1, 1, REAL '0.5', DECIMAL '0.10')", 1);
			String flagged = "SELECT * FROM typed WHERE flag";
			String flaggedValues = "VALUES (BIGINT '9007199254740993', "
					+ "2147483647, SMALLINT '-32768', TINYINT '127', DOUBLE '0.1234567890123456', REAL '2.5', "
					+ "CAST(DECIMAL '12345678.91' AS decimal(10, 2)), DECIMAL '1234567890123456789012345.12345', "
					+ "CAST('abc' AS varchar(10)), CAST('ab' AS char(3)), true, DATE '2024-01-02', "
					+ "TIMESTAMP '2024-01-02 03:04:05.006', TIMESTAMP '2024-01-02 03:04:05.006 UTC', "
					+ "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')";
			// matches checks the types the columns read back as, but compares doubles only to 5 significant digits
			assertThat(query(flagged)).matches(flaggedValues);
			RediSearchQueryRunner.assertExactRows(getQueryRunner(), flagged, flaggedValues);
			// NUMERIC fields hold doubles, which can't tell 2^53 + 1 from 2^53, so Trino compares bounds that large
			assertThat(query("SELECT i FROM typed WHERE b > 9007199254740992")).isNotFullyPushedDown(FilterNode.class)
					.matches("VALUES 2147483647");
			assertThat(query("SELECT i FROM typed WHERE b < 10")).isFullyPushedDown().matches("VALUES 1");
			assertThat(query("SELECT max(i), min(s), max(t), sum(r) FROM typed")).isFullyPushedDown()
					.matches("VALUES (2147483647, SMALLINT '-32768', TINYINT '127', REAL '3.0')");
			assertThat(query("SELECT t, count(*) FROM typed GROUP BY t")).isFullyPushedDown()
					.matches("VALUES (TINYINT '127', BIGINT '1'), (TINYINT '1', BIGINT '1')");
			// Redis would group decimals by their double values, so Trino groups them
			assertThat(query("SELECT dec, count(*) FROM typed GROUP BY dec")).matches("VALUES "
					+ "(CAST(DECIMAL '12345678.91' AS decimal(10, 2)), BIGINT '1'), (CAST(DECIMAL '0.10' AS decimal(10, 2)), BIGINT '1')");
			assertUpdate("ALTER TABLE typed ADD COLUMN extra bigint");
			redisearch.awaitIndexed("typed");
			assertUpdate("UPDATE typed SET extra = 42 WHERE b = 1", 1);
			assertThat(query("SELECT extra FROM typed WHERE b = 1")).matches("VALUES BIGINT '42'");
		} finally {
			assertUpdate("DROP TABLE typed");
		}
		assertThat(redisearch.getConnection().sync().exists(RediSearchColumnTypes.key("typed"))).isZero();
	}

	@Test
	public void testFiltersOnDeclaredTypes() {
		assertUpdate("CREATE TABLE typed_filters (id varchar, flag boolean, day date, u uuid, c char(3), r real, "
				+ "dec decimal(10, 2), longdec decimal(20, 2), ts timestamp(3), tstz timestamp(3) with time zone)");
		try {
			assertUpdate("INSERT INTO typed_filters VALUES "
					+ "('1', true, DATE '2024-01-02', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59', 'ab', REAL '1.1', "
					+ "DECIMAL '1.10', DECIMAL '1.10', TIMESTAMP '2024-01-02 03:04:05.006', "
					+ "TIMESTAMP '2024-01-02 03:04:05.006 UTC'), "
					+ "('2', false, DATE '2024-01-03', UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'cd', REAL '2.5', "
					+ "DECIMAL '2.50', DECIMAL '2.50', TIMESTAMP '2024-01-03 00:00:00.000', "
					+ "TIMESTAMP '2024-01-02 17:00:00.000 America/Denver'), "
					+ "('3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", 3);
			assertPushedDown("SELECT id FROM typed_filters WHERE flag", "VALUES VARCHAR '1'");
			// Trino keeps NOT flag as an expression rather than a domain
			assertPushedDown("SELECT id FROM typed_filters WHERE flag = false", "VALUES VARCHAR '2'");
			assertPushedDown("SELECT id FROM typed_filters WHERE day = DATE '2024-01-03'", "VALUES VARCHAR '2'");
			// Trino merges consecutive dates into a range, whose dates the tag query lists
			assertPushedDown("SELECT id FROM typed_filters WHERE day IN (DATE '2024-01-02', DATE '2024-01-03')",
					"VALUES VARCHAR '1', '2'");
			assertPushedDown("SELECT id FROM typed_filters WHERE day BETWEEN DATE '2024-01-01' AND DATE '2024-01-02'",
					"VALUES VARCHAR '1'");
			assertPushedDown("SELECT id FROM typed_filters WHERE u = UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'",
					"VALUES VARCHAR '1'");
			// Redis compares the double it parses from the text written for the float, 1.1
			assertPushedDown("SELECT id FROM typed_filters WHERE r = REAL '1.1'", "VALUES VARCHAR '1'");
			assertPushedDown("SELECT id FROM typed_filters WHERE r > REAL '1.1'", "VALUES VARCHAR '2'");
			assertPushedDown("SELECT id FROM typed_filters WHERE dec BETWEEN 1.1 AND 2", "VALUES VARCHAR '1'");
			assertPushedDown("SELECT id FROM typed_filters WHERE ts >= TIMESTAMP '2024-01-03 00:00:00.000'",
					"VALUES VARCHAR '2'");
			assertPushedDown("SELECT id FROM typed_filters WHERE tstz < TIMESTAMP '2024-01-03 00:00:00.000 UTC'",
					"VALUES VARCHAR '1'");
			// A FILTER keeps the equal rows, so Redis aggregates them too
			assertPushedDown("SELECT count(*) FROM typed_filters WHERE flag AND day = DATE '2024-01-02'",
					"VALUES BIGINT '1'");
			assertPushedDown("SELECT min(ts), max(tstz) FROM typed_filters", "VALUES (TIMESTAMP '2024-01-02 03:04:05.006', "
					+ "TIMESTAMP '2024-01-03 00:00:00.000 UTC')");
			// Redis prefilters CHAR values, whose padding it trims, and Trino compares them
			assertThat(query("SELECT id FROM typed_filters WHERE c = 'ab'")).isNotFullyPushedDown(FilterNode.class)
					.matches("VALUES VARCHAR '1'");
			assertThat((String) computeActual("EXPLAIN SELECT id FROM typed_filters WHERE c = 'ab'").getOnlyValue())
					.doesNotContain("constraint=ALL");
			// More digits than a double holds
			assertThat(query("SELECT id FROM typed_filters WHERE longdec = 1.10")).isNotFullyPushedDown(FilterNode.class)
					.matches("VALUES VARCHAR '1'");
		} finally {
			assertUpdate("DROP TABLE typed_filters");
		}
	}

	private void assertPushedDown(String sql, String expected) {
		assertThat(query(sql)).isFullyPushedDown().matches(expected);
	}

	@Test
	public void testExactAggregatesOfDeclaredTypes() {
		assertUpdate("CREATE TABLE exact_aggregates (g varchar, b bigint, r real)");
		try {
			assertUpdate("INSERT INTO exact_aggregates VALUES ('a', 1, REAL '0.1'), ('a', 2, REAL '0.2'), ('b', 2, REAL '0.1')",
					3);
			// An average of integers needn't be one, and Redis would round it to 12 digits
			assertExactAggregation("SELECT avg(b) FROM exact_aggregates", "VALUES DOUBLE '5' / 3");
			// A sharded database's coordinator gave two averages' counts the same name
			assertExactAggregation("SELECT g, avg(b), avg(r), sum(r) FROM exact_aggregates GROUP BY g",
					"VALUES (VARCHAR 'a', DOUBLE '1.5', REAL '0.15', REAL '0.3'), "
							+ "(VARCHAR 'b', DOUBLE '2', REAL '0.1', REAL '0.1')");
			// REAL decoding can collapse distinct external spellings, so grouping remains in Trino.
			assertThat(query("SELECT r, count(*) FROM exact_aggregates GROUP BY r"))
					.isNotFullyPushedDown(AggregationNode.class)
					.matches("VALUES (REAL '0.1', BIGINT '2'), (REAL '0.2', BIGINT '1')");
		} finally {
			assertUpdate("DROP TABLE exact_aggregates");
		}
	}

	@Test
	public void testAggregatesOfArithmeticOnDeclaredTypes() {
		assertUpdate("CREATE TABLE arithmetic (a bigint, b bigint, r real, x double)");
		try {
			assertUpdate("INSERT INTO arithmetic VALUES (2, 3, REAL '1.1', 0.5), (4, 5, REAL '2.5', 1.5)", 2);
			// A BIGINT cast to a double, which Redis parses the same
			assertExactAggregation("SELECT sum(a * x) FROM arithmetic", "VALUES DOUBLE '7'");
			// Integer arithmetic overflows in SQL but not in Redis, and Redis would compute REAL values as doubles
			assertThat(query("SELECT sum(a * b) FROM arithmetic")).isNotFullyPushedDown(ProjectNode.class)
					.matches("VALUES BIGINT '26'");
			assertThat(query("SELECT sum(r * x) FROM arithmetic")).isNotFullyPushedDown(ProjectNode.class)
					.matches("SELECT sum(CAST(REAL '1.1' AS double) * 0.5 + CAST(REAL '2.5' AS double) * 1.5)");
		} finally {
			assertUpdate("DROP TABLE arithmetic");
		}
	}

	private void assertExactAggregation(String sql, String expected) {
		assertThat(query(sql)).isFullyPushedDown();
		RediSearchQueryRunner.assertExactRows(getQueryRunner(), sql, expected);
	}

	@Test
	public void testNumericValuesAsStored() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("exact", CreateArgs.builder().withPrefix("exact:").build(),
				List.of(NumericFieldArgs.builder().name("d").build(), NumericFieldArgs.builder().name("sorted").sortable().build(),
						NumericFieldArgs.builder().name("raw_abv").as("abv").build(), TagFieldArgs.builder().name("style").build()));
		redisearch.awaitIndexed("exact");
		redis.hset("exact:1", Map.of("d", "0.1234567890123456", "sorted", "229577310901.21", "raw_abv",
				"4.123456789012345", "style", "Wheat", "note", "n1"));
		redis.hset("exact:2", Map.of("d", "1.0E-7", "sorted", "2", "raw_abv", "5", "style", "Ale"));
		// Redis rounds NUMERIC values loaded by name to 12 significant digits, so the hash's fields are loaded as stored
		String wheat = "SELECT d, sorted, abv, note FROM exact WHERE style = 'Wheat'";
		assertThat(query(wheat)).isFullyPushedDown();
		RediSearchQueryRunner.assertExactRows(getQueryRunner(), wheat, "VALUES (DOUBLE '0.1234567890123456', "
				+ "DOUBLE '229577310901.21', DOUBLE '4.123456789012345', VARCHAR 'n1')");
		String small = "SELECT __key, d FROM exact WHERE d < 0.1 LIMIT 5";
		assertThat(query(small)).isFullyPushedDown();
		RediSearchQueryRunner.assertExactRows(getQueryRunner(), small, "VALUES (VARCHAR 'exact:2', DOUBLE '1.0E-7')");
		assertThat(query("SELECT style FROM exact WHERE sorted = 229577310901.21")).matches("VALUES VARCHAR 'Wheat'");
	}

	@Test
	public void testBigintValuesAsStored() {
		assertUpdate("CREATE TABLE big (id varchar, b bigint)");
		try {
			assertUpdate("INSERT INTO big VALUES ('max', 9223372036854775807), ('min', -9223372036854775808), "
					+ "('above', 9007199254740993), ('below', -9007199254740993), ('exact', 9007199254740991), "
					+ "('small', -7), ('none', NULL)", 7);
			// Loaded by name, and read again from the hash where Redis may have rounded the value
			assertThat(query("SELECT id, b FROM big")).matches("VALUES (VARCHAR 'max', BIGINT '9223372036854775807'), "
					+ "('min', BIGINT '-9223372036854775808'), ('above', BIGINT '9007199254740993'), "
					+ "('below', BIGINT '-9007199254740993'), ('exact', BIGINT '9007199254740991'), ('small', BIGINT '-7'), "
					+ "('none', CAST(NULL AS bigint))");
		} finally {
			assertUpdate("DROP TABLE big");
		}
		// A field indexed AS another name is read again from its hash field
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("bigalias", CreateArgs.builder().withPrefix("bigalias:").build(),
				List.of(NumericFieldArgs.builder().name("raw_b").as("b").build()));
		RediSearchColumnTypes.write(redis, "bigalias", Map.of("b", BigintType.BIGINT));
		redisearch.awaitIndexed("bigalias");
		redis.hset("bigalias:1", Map.of("raw_b", "9007199254740993"));
		assertThat(query("SELECT b FROM bigalias")).matches("VALUES BIGINT '9007199254740993'");
	}

	@Test
	public void testJsonValuesAsStored() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("jsonexact", CreateArgs.builder().on(TargetType.JSON).withPrefix("jsonexact:").build(),
				List.of(NumericFieldArgs.builder().name("$.score").as("score").build(),
						NumericFieldArgs.builder().name("$.n.x").as("x").build(),
						TagFieldArgs.builder().name("$.flag").as("flag").build(),
						TagFieldArgs.builder().name("$.tags[*]").as("tags").build(),
						TextFieldArgs.builder().name("$.name").as("name").build(),
						TagFieldArgs.builder().name("$.missing").as("missing").build()));
		redisearch.awaitIndexed("jsonexact");
		String document = "{\"score\":0.1234567890123456,\"n\":{\"x\":229577310901.21},\"flag\":true,"
				+ "\"tags\":[\"a\",\"b,c\"],\"name\":\"He said \\\"hi\\\"\"}";
		redis.jsonSet("jsonexact:1", JsonPath.ROOT_PATH, document);
		// DIALECT 3 returns numbers as stored. The other values are what DIALECT 2 returned: a boolean as 1, and the
		// first value of a path that matches several.
		RediSearchQueryRunner.assertExactRows(getQueryRunner(), "SELECT score, x, flag, tags, name, missing FROM jsonexact",
				"VALUES (DOUBLE '0.1234567890123456', DOUBLE '229577310901.21', VARCHAR '1', VARCHAR 'a', "
						+ "VARCHAR 'He said \"hi\"', CAST(NULL AS varchar))");
		assertThat(query("SELECT \"$\" FROM jsonexact")).matches("VALUES VARCHAR '" + document + "'");
	}

	@Test
	public void testSavedTypesThatDontApply() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		// Saved for fields of other types, as when an index is dropped and created again outside Trino; or not JSON
		createIndex("stale", "{\"n\": \"date\", \"t\": \"bigint\"}");
		createIndex("malformed", "not json");
		for (String index : List.of("stale", "malformed")) {
			redis.hset(index + ":1", Map.of("n", "4.5", "t", "x"));
			assertThat(query("SELECT n, t FROM " + index)).matches("VALUES (DOUBLE '4.5', VARCHAR 'x')");
		}
		// A saved type applies to values other clients write too
		createIndex("declared", "{\"n\": \"bigint\"}");
		redis.hset("declared:1", Map.of("n", "42.0", "t", "x"));
		assertThat(query("SELECT n, t FROM declared")).matches("VALUES (BIGINT '42', VARCHAR 'x')");
		redis.hset("declared:2", Map.of("n", "4.5", "t", "y"));
		assertQueryFails("SELECT n FROM declared", "Value '4.5' is not a valid bigint");
	}

	private void createIndex(String index, String columnTypes) {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.set(RediSearchColumnTypes.key(index), columnTypes);
		redis.ftCreate(index, CreateArgs.builder().withPrefix(index + ":").build(),
				List.of(NumericFieldArgs.builder().name("n").build(), TagFieldArgs.builder().name("t").build()));
		redisearch.awaitIndexed(index);
	}

	@Test
	public void testUnsupportedColumnTypes() {
		assertQueryFails("CREATE TABLE unsupported_types AS SELECT INTERVAL '1' DAY a",
				"Unsupported column type: interval day\\(1\\)");
		assertThat(computeActual("SHOW TABLES LIKE 'unsupported_types'").getRowCount()).isZero();
	}

	@Test
	public void testJsonIndexWrites() {
		assertUpdate("INSERT INTO docs (id, message, score) VALUES ('9', 'json insert', 1)", 1);
		assertUpdate("UPDATE docs SET message = 'changed', score = 2 WHERE id = '9'", 1);
		assertThat(query("SELECT message, score FROM docs WHERE id = '9'"))
				.matches("VALUES (VARCHAR 'changed', DOUBLE '2')");
		assertUpdate("UPDATE docs SET message = NULL WHERE id = '9'", 1);
		assertThat(query("SELECT message FROM docs WHERE id = '9'"))
				.matches("VALUES CAST(NULL AS varchar)");
		assertUpdate("MERGE INTO docs t USING (VALUES ('9', 5), ('10', 6)) s(id, score) ON t.id = s.id "
				+ "WHEN MATCHED THEN UPDATE SET score = s.score "
				+ "WHEN NOT MATCHED THEN INSERT (id, score) VALUES (s.id, s.score)", 2);
		assertThat(query("SELECT id, score FROM docs WHERE id IN ('9', '10')"))
				.matches("VALUES (VARCHAR '9', DOUBLE '5'), (VARCHAR '10', DOUBLE '6')");
		assertUpdate("DELETE FROM docs WHERE id IN ('9', '10')", 2);
		assertThat(query("SELECT id, message, score FROM docs"))
				.matches("VALUES (VARCHAR '1', VARCHAR 'json doc', DOUBLE '3')");
	}

	@Test
	public void testJsonInsertSelect() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("json_insert_select", CreateArgs.builder().on(TargetType.JSON).withPrefix("json_select:").build(),
				List.of(TagFieldArgs.builder().name("$.id").as("id").build(),
						TagFieldArgs.builder().name("$.profile.name").as("display_name").build(),
						NumericFieldArgs.builder().name("$.score").as("score").build()));
		redisearch.awaitIndexed("json_insert_select");
		try {
			assertUpdate("INSERT INTO json_insert_select (id, display_name, score) "
					+ "SELECT id, name, score FROM (VALUES ('a', 'Ada', DOUBLE '1.5'), "
					+ "('b', CAST(NULL AS varchar), DOUBLE '2.5')) s(id, name, score)", 2);
			assertThat(query("SELECT id, display_name, score FROM json_insert_select"))
					.matches("VALUES (VARCHAR 'a', VARCHAR 'Ada', DOUBLE '1.5'), (VARCHAR 'b', CAST(NULL AS varchar), DOUBLE '2.5')");
			assertUpdate("INSERT INTO json_insert_select (id, score) "
					+ "SELECT id || '_copy', score + 1 FROM json_insert_select", 2);
			assertThat(query("SELECT id, display_name, score FROM json_insert_select WHERE id LIKE '%_copy'"))
					.matches("VALUES (VARCHAR 'a_copy', CAST(NULL AS varchar), DOUBLE '2.5'), "
							+ "(VARCHAR 'b_copy', CAST(NULL AS varchar), DOUBLE '3.5')");
		} finally {
			assertUpdate("DROP TABLE json_insert_select");
		}
	}

	@Test
	public void testJsonMergeFailurePropagatesWithoutRollingBackCompletedWrites() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		String index = "json_partial_failure";
		redis.ftCreate(index, CreateArgs.builder().on(TargetType.JSON).withPrefix(index + ":").build(),
				List.of(NumericFieldArgs.builder().name("$.score").as("score").build()));
		for (String key : List.of("first", "second", "failed", "untouched")) {
			redis.jsonSet(index + ":" + key, JsonPath.ROOT_PATH, "{\"score\":0,\"keep\":\"yes\"}");
		}
		redisearch.awaitIndexed(index);
		RediSearchSession session = new RediSearchSession(
				getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager(),
				new RediSearchConfig().setUri(redisearch.getRedisURI()).setCluster(deployment().isCluster()));
		SchemaTableName name = new SchemaTableName("tpch", index);
		var columns = List.of(new RediSearchColumnHandle("score", DOUBLE, RediSearchFieldType.NUMERIC,
				false, true, Optional.empty()));
		var sink = new RediSearchMergeSink(session, new RediSearchPageSink(session, name, columns),
				new RediSearchMergeTableHandle(new RediSearchTableHandle(name, index), columns, Map.of(0, List.of(0))));
		try {
			sink.storeMergedRows(jsonUpdatePage(index, List.of("first"), 1));
			// A bad later value must be rejected before any mutation in that page is sent.
			assertThatThrownBy(() -> sink.storeMergedRows(jsonUpdatePage(index, List.of("second", "untouched"), 2, Double.NaN)))
					.hasMessageContaining("finite number");
			assertThat(redis.jsonGetRaw(index + ":second", JsonPath.ROOT_PATH).toString()).contains("\"score\":0");
			assertThat(redis.jsonGetRaw(index + ":untouched", JsonPath.ROOT_PATH).toString()).contains("\"score\":0");
			// Inject a real Redis command error after schema discovery, rather than mocking a future.
			redis.del(index + ":failed");
			redis.set(index + ":failed", "concurrent external writer changed the key type");
			assertThatThrownBy(() -> sink.storeMergedRows(jsonUpdatePage(index, List.of("second", "failed"), 2, 3)))
					.isInstanceOf(io.lettuce.core.RedisCommandExecutionException.class)
					.hasMessageMatching(".*(WRONGTYPE|Existing key has wrong Redis type).*");
			sink.abort();
			assertThat(redis.jsonGetRaw(index + ":first", JsonPath.ROOT_PATH).toString()).contains("\"score\":1", "\"keep\":\"yes\"");
			assertThat(redis.jsonGetRaw(index + ":second", JsonPath.ROOT_PATH).toString()).contains("\"score\":2", "\"keep\":\"yes\"");
			assertThat(redis.get(index + ":failed")).isEqualTo("concurrent external writer changed the key type");
			assertThat(redis.jsonGetRaw(index + ":untouched", JsonPath.ROOT_PATH).toString()).contains("\"score\":0");
		} finally {
			sink.abort();
			session.shutdown();
			for (String key : List.of("first", "second", "failed", "untouched")) {
				redis.del(index + ":" + key);
			}
			redis.ftDropindex(index);
		}
	}

	private static Page jsonUpdatePage(String index, List<String> suffixes, double... scores) {
		PageBuilder page = new PageBuilder(List.of(DOUBLE, TINYINT, INTEGER, VARCHAR));
		for (int position = 0; position < suffixes.size(); position++) {
			page.declarePosition();
			DOUBLE.writeDouble(page.getBlockBuilder(0), scores[position]);
			TINYINT.writeLong(page.getBlockBuilder(1), UPDATE_OPERATION_NUMBER);
			INTEGER.writeLong(page.getBlockBuilder(2), 0);
			VARCHAR.writeString(page.getBlockBuilder(3), index + ":" + suffixes.get(position));
		}
		return page.build();
	}

	@Test
	public void testJsonNestedWritesPreserveUnrelatedProperties() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("nested_writes", CreateArgs.builder().on(TargetType.JSON).withPrefix("nested_write:").build(),
				List.of(TagFieldArgs.builder().name("$.id").as("id").build(),
						TagFieldArgs.builder().name("$.profile.name").as("display_name").build()));
		redisearch.awaitIndexed("nested_writes");
		redis.jsonSet("nested_write:existing", JsonPath.ROOT_PATH,
				"{\"id\":\"existing\",\"profile\":{\"name\":\"old\",\"keep\":\"yes\"},\"other\":17}");
		try {
			assertUpdate("INSERT INTO nested_writes (id, display_name) VALUES ('new', 'Ada')", 1);
			assertUpdate("UPDATE nested_writes SET display_name = 'Grace' WHERE id = 'existing'", 1);
			assertThat(redis.jsonGetRaw("nested_write:existing", JsonPath.ROOT_PATH).toString())
					.contains("keep", "yes", "other", "17", "Grace");
			assertUpdate("UPDATE nested_writes SET display_name = NULL WHERE id = 'existing'", 1);
			assertThat(query("SELECT id, display_name FROM nested_writes"))
					.matches("VALUES (VARCHAR 'existing', CAST(NULL AS varchar)), (VARCHAR 'new', VARCHAR 'Ada')");
			redis.jsonSet("nested_write:missing", JsonPath.ROOT_PATH, "{\"id\":\"missing\"}");
			assertUpdate("UPDATE nested_writes SET display_name = 'created' WHERE id = 'missing'", 1);
			assertThat(query("SELECT display_name FROM nested_writes WHERE id = 'missing'"))
					.matches("VALUES VARCHAR 'created'");
			assertQueryFails("INSERT INTO nested_writes (\"$\") VALUES ('{}')",
					"JSON writes require an indexed scalar path for column: \\$");
		} finally {
			assertUpdate("DROP TABLE nested_writes");
		}
	}

	@Test
	public void testEncodedScalarTypes() {
		assertUpdate("CREATE TABLE encoded_types (b varbinary, t time(12), ts timestamp(6), early timestamp(0))");
		try {
			assertUpdate("INSERT INTO encoded_types VALUES (X'00FF8041', TIME '23:59:59.123456789012', "
					+ "TIMESTAMP '1969-12-31 23:59:59.123456', TIMESTAMP '2024-01-02 03:04:05')", 1);
			assertThat(query("SELECT * FROM encoded_types"))
					.matches("VALUES (X'00FF8041', TIME '23:59:59.123456789012', "
							+ "TIMESTAMP '1969-12-31 23:59:59.123456', TIMESTAMP '2024-01-02 03:04:05')");
			assertThat(query("SELECT count(*) FROM encoded_types WHERE b = X'00FF8041' AND "
					+ "t > TIME '20:00:00' AND ts < TIMESTAMP '1970-01-01 00:00:00'"))
					.matches("VALUES BIGINT '1'");
		} finally {
			assertUpdate("DROP TABLE encoded_types");
		}
	}
	@Test
	public void testComplexAndFullPrecisionTypes() {
		assertUpdate("CREATE TABLE complex_types (a array(bigint), m map(integer, array(varchar)), "
				+ "r row(x bigint, y varchar), j json, ts timestamp(12), z timestamp(12) with time zone, "
				+ "t time(12) with time zone)");
		String values = "(ARRAY[BIGINT '9007199254740993', NULL], CAST(MAP(ARRAY[1], ARRAY[ARRAY['x', NULL]]) AS MAP(INTEGER, ARRAY(VARCHAR))), "
				+ "CAST(ROW(BIGINT '9007199254740993', 'y') AS ROW(x bigint, y varchar)), JSON '{\"a\":[true,null]}', "
				+ "TIMESTAMP '1969-12-31 23:59:59.123456789012', TIMESTAMP '2024-01-02 03:04:05.123456789012 UTC', "
				+ "TIME '12:34:56.123456789012-07:00')";
		try {
			assertUpdate("INSERT INTO complex_types VALUES " + values, 1);
			assertThat(query("SELECT a, m, r, json_format(j), ts, z, t FROM complex_types"))
					.matches("SELECT a, m, r, json_format(j), ts, z, t FROM (VALUES " + values + ") v(a,m,r,j,ts,z,t)");
			assertUpdate("CREATE TABLE complex_copy AS SELECT * FROM complex_types", 1);
			try {
				assertThat(query("SELECT a, m, r, json_format(j), ts, z, t FROM complex_copy"))
						.matches("SELECT a, m, r, json_format(j), ts, z, t FROM complex_types");
			} finally {
				assertUpdate("DROP TABLE complex_copy");
			}
		} finally {
			assertUpdate("DROP TABLE complex_types");
		}
	}

	@Test
	public void testNullOnlyHashRowsSurvive() {
		assertUpdate("CREATE TABLE null_only_rows (x bigint)");
		try {
			assertUpdate("INSERT INTO null_only_rows VALUES (NULL), (1)", 2);
			assertThat(query("SELECT x FROM null_only_rows"))
					.matches("VALUES CAST(NULL AS bigint), BIGINT '1'");
			assertUpdate("UPDATE null_only_rows SET x = NULL WHERE x = 1", 1);
			assertThat(query("SELECT * FROM null_only_rows"))
					.matches("VALUES CAST(NULL AS bigint), CAST(NULL AS bigint)");
			assertThat(query("SHOW COLUMNS FROM null_only_rows"))
					.matches("SELECT CAST('x' AS varchar), CAST('bigint' AS varchar), CAST('' AS varchar), CAST('' AS varchar)");
			assertThat(query("SELECT count(*) FROM null_only_rows WHERE x IS NULL")).matches("VALUES BIGINT '2'");
		} finally {
			assertUpdate("DROP TABLE null_only_rows");
		}
	}

	@Test
	public void testReservedHashMarkerCannotOverwriteApplicationFields() {
		assertQueryFails("CREATE TABLE marker_reserved (__trino_row_marker varchar)",
				"Column name is reserved for connector row storage: __trino_row_marker");
		assertUpdate("CREATE TABLE marker_reserved (id varchar)");
		try {
			assertQueryFails("ALTER TABLE marker_reserved ADD COLUMN __trino_row_marker varchar",
					"Column name is reserved for connector row storage: __trino_row_marker");
		} finally {
			assertUpdate("DROP TABLE marker_reserved");
		}
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("marker_external", CreateArgs.builder().withPrefix("marker_external:").build(),
				List.of(TagFieldArgs.builder().name(RediSearchSession.ROW_MARKER).as("application_value").build()));
		redisearch.awaitIndexed("marker_external");
		redis.hset("marker_external:1", RediSearchSession.ROW_MARKER, "application data");
		try {
			assertQueryFails("INSERT INTO marker_external (application_value) VALUES ('overwrite')",
					"HASH writes cannot overwrite the reserved row marker field: __trino_row_marker");
			assertQueryFails("UPDATE marker_external SET application_value = 'overwrite'",
					"HASH writes cannot overwrite the reserved row marker field: __trino_row_marker");
			assertThat(redis.hget("marker_external:1", RediSearchSession.ROW_MARKER)).isEqualTo("application data");
		} finally {
			assertUpdate("DROP TABLE marker_external");
		}
	}

	@Test
	public void testHashWritesResolveIndexedAliases() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("hash_alias_writes", CreateArgs.builder().withPrefix("hash_alias_write:").build(),
				List.of(TagFieldArgs.builder().name("raw_name").as("sql_name").build()));
		redisearch.awaitIndexed("hash_alias_writes");
		redis.hset("hash_alias_write:seed", "raw_name", "seed");
		try {
			assertUpdate("INSERT INTO hash_alias_writes (sql_name) VALUES ('new')", 1);
			String key = (String) computeActual("SELECT __key FROM hash_alias_writes WHERE sql_name = 'new'").getOnlyValue();
			assertThat(redis.hget(key, "raw_name")).isEqualTo("new");
			assertThat(redis.hexists(key, "sql_name")).isFalse();
			assertUpdate("UPDATE hash_alias_writes SET sql_name = 'changed' WHERE sql_name = 'new'", 1);
			assertThat(redis.hget(key, "raw_name")).isEqualTo("changed");
			assertUpdate("UPDATE hash_alias_writes SET sql_name = NULL WHERE sql_name = 'changed'", 1);
			assertThat(redis.hget(key, "raw_name")).isNull();
			assertThat(query("SELECT count(*) FROM hash_alias_writes WHERE sql_name IS NULL")).matches("VALUES BIGINT '1'");
			assertQueryFails("INSERT INTO hash_alias_writes (sql_name, raw_name) VALUES ('a', 'b')",
					"Conflicting assignments to aliases of HASH field: raw_name");
			assertQueryFails("UPDATE hash_alias_writes SET sql_name = 'a', raw_name = 'b' WHERE sql_name = 'seed'",
					"Conflicting assignments to aliases of HASH field: raw_name");
			assertThat(redis.hget("hash_alias_write:seed", "raw_name")).isEqualTo("seed");
		} finally {
			assertUpdate("DROP TABLE hash_alias_writes");
		}
	}

}
