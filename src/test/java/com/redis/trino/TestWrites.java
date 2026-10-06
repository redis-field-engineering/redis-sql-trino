package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

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
import io.trino.sql.planner.plan.FilterNode;
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
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch);
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
			assertExactAggregation("SELECT r, count(*) FROM exact_aggregates GROUP BY r",
					"VALUES (REAL '0.1', BIGINT '2'), (REAL '0.2', BIGINT '1')");
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
		assertQueryFails("CREATE TABLE unsupported_types (t time)", "Unsupported column type: time\\(3\\)");
		assertQueryFails("CREATE TABLE unsupported_types AS SELECT ARRAY[1] a",
				"Unsupported column type: array\\(integer\\)");
		assertThat(computeActual("SHOW TABLES LIKE 'unsupported_types'").getRowCount()).isZero();
	}

	@Test
	public void testJsonIndexWrites() {
		// The connector writes hashes, which a JSON index never sees
		assertQueryFails("INSERT INTO docs (id, message, score) VALUES ('9', 'json insert', 1)",
				"Index docs is on JSON documents; the connector only writes hashes, so only DELETE is supported");
		assertQueryFails("UPDATE docs SET message = 'changed' WHERE id = '1'",
				"Index docs is on JSON documents; the connector only writes hashes, so only DELETE is supported");
		assertQueryFails("MERGE INTO docs t USING (VALUES ('9')) s(id) ON t.id = s.id "
				+ "WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
				"Index docs is on JSON documents; the connector only writes hashes, so only DELETE is supported");
		assertThat(redisearch.getConnection().sync().keys("docs:*")).isEmpty();
		assertThat(query("SELECT id, message, score FROM docs"))
				.matches("VALUES (VARCHAR '1', VARCHAR 'json doc', DOUBLE '3')");
	}
}
