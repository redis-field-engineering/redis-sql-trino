package com.redis.trino;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.base.Throwables;
import com.redis.lettucemod.Beers;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.Field;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;

public class TestRediSearchConnectorSmokeTest extends BaseConnectorSmokeTest {

	private static final Logger log = Logger.get(TestRediSearchConnectorSmokeTest.class);

	private RediSearchServer redisearch;

	private void populateBeers() throws IOException {
		try {
			redisearch.getTestContext().sync().ftDropindexDeleteDocs(Beers.INDEX);
		} catch (Exception e) {
			// ignore
		}
		Beers.populateIndex(redisearch.getTestContext().getConnection());
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

	@Override
	protected void assertQuery(String sql) {
		log.info("assertQuery: %s", sql);
		super.assertQuery(sql);
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = new RediSearchServer();
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, CUSTOMER, NATION, ORDERS, REGION);
	}

	@Test
	public void testNonIndexedFields() throws IOException {
		populateBeers();
		getQueryRunner().execute("select id, last_mod from beers");
	}

	@Test
	public void testBuiltinFields() throws IOException {
		populateBeers();
		getQueryRunner().execute("select _id, _score from beers");
	}

	@Test
	public void testCountEmptyIndex() throws IOException {
		try {
			redisearch.getTestContext().sync().ftDropindexDeleteDocs(Beers.INDEX);
		} catch (Exception e) {
			// ignore
		}
		Beers.createIndex(redisearch.getTestContext().getConnection());
		assertQuery("SELECT count(*) FROM beers", "VALUES 0");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testJsonSearch() throws IOException {
		RedisModulesCommands<String, String> sync = redisearch.getTestContext().getConnection().sync();
		sync.ftCreate("jsontest", CreateOptions.<String, String>builder().on(DataType.JSON).build(),
				Field.tag("$.id").as("id").build(), Field.text("$.message").as("message").build());
		sync.jsonSet("doc:1", "$", "{\"id\": \"1\", \"message\": \"this is a test\"}");
		sync.jsonSet("doc:2", "$", "{\"id\": \"2\", \"message\": \"this is another test\"}");
		getQueryRunner().execute("select id, message from jsontest");
	}

	@Test
	public void testHaving() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testShowCreateTable() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Test
	public void testInsertIndex() throws IOException {
		try {
			redisearch.getTestContext().sync().ftDropindexDeleteDocs(Beers.INDEX);
		} catch (Exception e) {
			// ignore
		}
		Beers.createIndex(redisearch.getTestContext().getConnection());
		assertUpdate("INSERT INTO beers (id, name) VALUES ('abc', 'mybeer')", 1);
		assertThat(query("SELECT id, name FROM beers")).matches("VALUES (VARCHAR 'abc', VARCHAR 'mybeer')");
		List<String> keys = redisearch.getTestContext().sync().keys("beer:*");
		assertEquals(keys.size(), 1);
		assertTrue(keys.get(0).startsWith("beer:"));
	}

	@AfterClass(alwaysRun = true)
	public final void destroy() {
		redisearch.close();
	}

	static RuntimeException getTrinoExceptionCause(Throwable e) {
		return Throwables.getCausalChain(e).stream().filter(TestRediSearchConnectorSmokeTest::isTrinoException)
				.findFirst().map(RuntimeException.class::cast)
				.orElseThrow(() -> new IllegalArgumentException("Exception does not have TrinoException cause", e));
	}

	private static boolean isTrinoException(Throwable exception) {
		requireNonNull(exception, "exception is null");

		if (exception instanceof TrinoException || exception instanceof ParsingException) {
			return true;
		}

		if (exception.getClass().getName().equals("io.trino.client.FailureInfo$FailureException")) {
			try {
				String originalClassName = exception.toString().split(":", 2)[0];
				Class<? extends Throwable> originalClass = Class.forName(originalClassName).asSubclass(Throwable.class);
				return TrinoException.class.isAssignableFrom(originalClass)
						|| ParsingException.class.isAssignableFrom(originalClass);
			} catch (ClassNotFoundException e) {
				return false;
			}
		}

		return false;
	}

	@Test
	public void testLikePredicate() {
		assertQuery("SELECT name, regionkey FROM nation WHERE name LIKE 'EGY%'");
	}

	@Test
	public void testInPredicate() {
		assertQuery("SELECT name, regionkey FROM nation WHERE name in ('EGYPT', 'FRANCE')");
	}

	@Test
	public void testInPredicateNumeric() {
		assertQuery("SELECT name, regionkey FROM nation WHERE regionKey in (1, 2, 3)");
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
