package com.redis.trino;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.MessageFormat;
import java.util.Arrays;

import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;

public class TestCaseInsensitiveConnectorSmokeTest extends BaseConnectorSmokeTest {

	private RediSearchServer redisearch;

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = new RediSearchServer();
		redisearch.getConnection().sync().flushall();
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch,
				Arrays.asList(CUSTOMER, NATION, ORDERS, REGION), ImmutableMap.of(),
				ImmutableMap.of("redisearch.case-insensitive-names", "true"));
	}

	@AfterClass(alwaysRun = true)
	public final void destroy() {
		redisearch.close();
	}

	@Override
	protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
		switch (connectorBehavior) {
		case SUPPORTS_CREATE_SCHEMA:
		case SUPPORTS_CREATE_VIEW:
			return false;

		case SUPPORTS_CREATE_TABLE:
			return true;

		case SUPPORTS_ARRAY:
			return false;

		case SUPPORTS_DROP_COLUMN:
		case SUPPORTS_RENAME_COLUMN:
		case SUPPORTS_RENAME_TABLE:
			return false;

		case SUPPORTS_COMMENT_ON_TABLE:
		case SUPPORTS_COMMENT_ON_COLUMN:
			return false;

		case SUPPORTS_TOPN_PUSHDOWN:
			return false;

		case SUPPORTS_NOT_NULL_CONSTRAINT:
			return false;

		case SUPPORTS_DELETE:
		case SUPPORTS_INSERT:
		case SUPPORTS_UPDATE:
			return true;

		case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
			return false;

		case SUPPORTS_LIMIT_PUSHDOWN:
			return true;

		case SUPPORTS_PREDICATE_PUSHDOWN:
		case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
			return true;

		case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
			return false;

		case SUPPORTS_NEGATIVE_DATE:
			return false;

		default:
			return super.hasBehavior(connectorBehavior);
		}
	}

	@Test
	public void testHaving() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testShowCreateTable() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMixedCaseIndex() {
		RedisModulesCommands<String, String> redis = redisearch.getConnection().sync();
		CreateOptions.Builder<String, String> options = CreateOptions.builder();
		String prefix = "beer:";
		options.prefix(prefix);
		redis.hset(prefix + "1", "id", "1");
		redis.hset(prefix + "1", "name", "MyBeer");
		redis.hset(prefix + "2", "id", "2");
		redis.hset(prefix + "2", "name", "MyOtherBeer");
		String index = "MixedCaseBeers";
		redis.ftCreate(index, options.build(), Field.tag("id").build(), Field.tag("name").build());
		getQueryRunner().execute(MessageFormat.format("SELECT * FROM {0}", index));
		getQueryRunner().execute(MessageFormat.format("SELECT * FROM {0}", index.toLowerCase()));
		getQueryRunner().execute(MessageFormat.format("SELECT * FROM {0}", index.toUpperCase()));
	}

	@SuppressWarnings("resource")
	@Test
	public void testUpdate() {
		if (!hasBehavior(SUPPORTS_UPDATE)) {
			// Note this change is a no-op, if actually run
			assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1",
					"This connector does not support updates");
			return;
		}

		if (!hasBehavior(SUPPORTS_INSERT)) {
			throw new AssertionError("Cannot test UPDATE without INSERT");
		}

		try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_",
				getCreateTableDefaultDefinition())) {
			assertUpdate("INSERT INTO " + table.getName() + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region",
					"SELECT count(*) FROM region");
			assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
					.matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

			assertUpdate("UPDATE " + table.getName() + " SET b = b + 1.2 WHERE a % 2 = 0", 3);
			assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
					.matches(expectedValues("(0, 1.2), (1, 2.5), (2, 6.2), (3, 7.5), (4, 11.2)"));
		}
	}

}
