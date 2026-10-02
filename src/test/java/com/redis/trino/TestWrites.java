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
import io.trino.sql.planner.plan.FilterNode;
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
			// Redis returns NUMERIC values as doubles, rounded to 12 significant digits unless they're integers, so
			// these are values it returns exactly: 2^53 + 4 is a double, and 2^53 + 3 isn't
			assertUpdate("INSERT INTO typed VALUES (BIGINT '9007199254740996', 2147483647, SMALLINT '-32768', "
					+ "TINYINT '127', 1.5E0, REAL '2.5', DECIMAL '12345678.91', DECIMAL '1234567.12345', "
					+ "'abc', 'ab', true, DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05.006', "
					+ "TIMESTAMP '2024-01-02 03:04:05.006 UTC', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')", 1);
			assertUpdate("INSERT INTO typed (b, i, s, t, r, dec) VALUES (1, 1, 1, 1, REAL '0.5', DECIMAL '0.10')", 1);
			assertThat(query("SELECT * FROM typed WHERE flag")).matches("VALUES (BIGINT '9007199254740996', "
					+ "2147483647, SMALLINT '-32768', TINYINT '127', DOUBLE '1.5', REAL '2.5', "
					+ "CAST(DECIMAL '12345678.91' AS decimal(10, 2)), CAST(DECIMAL '1234567.12345' AS decimal(30, 5)), "
					+ "CAST('abc' AS varchar(10)), CAST('ab' AS char(3)), true, DATE '2024-01-02', "
					+ "TIMESTAMP '2024-01-02 03:04:05.006', TIMESTAMP '2024-01-02 03:04:05.006 UTC', "
					+ "UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')");
			// As a double, the bound 2^53 + 3 would be 2^53 + 4, so Trino compares bounds that large
			assertThat(query("SELECT i FROM typed WHERE b > 9007199254740995")).isNotFullyPushedDown(FilterNode.class)
					.matches("VALUES 2147483647");
			assertThat(query("SELECT i FROM typed WHERE b < 10")).isFullyPushedDown().matches("VALUES 1");
			assertThat(query("SELECT max(b), min(s), max(t), sum(r) FROM typed")).isFullyPushedDown()
					.matches("VALUES (BIGINT '9007199254740996', SMALLINT '-32768', TINYINT '127', REAL '3.0')");
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
