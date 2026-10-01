package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.CreateArgs.TargetType;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
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
			// Columns are read back from the index: TAG fields as VARCHAR, NUMERIC fields as DOUBLE
			assertThat(query("SELECT id, flag, day, uuid, price, quantity FROM ctas_types"))
					.matches("VALUES (VARCHAR 'a', VARCHAR 'true', VARCHAR '2024-01-02', "
							+ "VARCHAR '12151fd2-7586-11e9-8f9e-2a86e4085a59', DOUBLE '1.25', DOUBLE '7')");
			assertThat(query("SELECT id FROM ctas_types WHERE flag = 'true' AND day = '2024-01-02'"))
					.matches("VALUES VARCHAR 'a'");
			assertThat(query("SELECT ts FROM ctas_types")).matches("VALUES DOUBLE '1704164645006'");
		} finally {
			assertUpdate("DROP TABLE ctas_types");
		}
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
