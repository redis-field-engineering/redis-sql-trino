package com.redis.trino;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.base.Throwables;

import io.airlift.log.Logger;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.CreateArgs.TargetType;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

public class TestConnectorSmokeTest extends BaseConnectorSmokeTest {

	private static final Logger log = Logger.get(TestConnectorSmokeTest.class);

	private RediSearchServer redisearch;

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = closeAfterClass(new RediSearchServer());
		redisearch.getConnection().sync().flushall();
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, CUSTOMER, NATION, ORDERS, REGION);
	}

	// A small stand-in for the lettucemod "beers" test dataset: indexed fields plus an unindexed last_mod
	private void populateBeers() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("beers", CreateArgs.builder().withPrefix("beer:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("brewery_id").build(),
						TextFieldArgs.builder().name("name").build(), NumericFieldArgs.builder().name("abv").build(),
						NumericFieldArgs.builder().name("ibu").build(), TextFieldArgs.builder().name("descript").build(),
						TagFieldArgs.builder().name("style_name").build(), TagFieldArgs.builder().name("cat_name").build()));
		redisearch.awaitIndexed("beers");
		redis.hset("beer:1", Map.of("id", "1", "brewery_id", "812", "name", "Hocus Pocus", "abv", "4.5", "ibu", "0",
				"style_name", "Light American Wheat Ale or Lager", "cat_name", "Other Style", "last_mod",
				"2010-07-22 20:00:20 UTC"));
		redis.hset("beer:2", Map.of("id", "2", "brewery_id", "812", "name", "Grimm's Witbier", "abv", "5.0", "ibu",
				"0", "style_name", "Belgian-Style White", "cat_name", "Belgian and French Ale", "last_mod",
				"2010-07-22 20:00:20 UTC"));
	}

	@Override
	protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
		switch (connectorBehavior) {
		case SUPPORTS_CREATE_SCHEMA:
		case SUPPORTS_CREATE_VIEW:
		case SUPPORTS_CREATE_MATERIALIZED_VIEW:
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
		case SUPPORTS_MERGE:
			return true;

		case SUPPORTS_TRUNCATE:
			return false;

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

	@Test
	public void testRediSearchFields() {
		populateBeers();
		getQueryRunner().execute("select id, last_mod from beers");
		getQueryRunner().execute("select __key from beers");
	}

	@Test
	public void testCountEmptyIndex() {
		String index = "emptyidx";
		redisearch.getConnection().sync().ftCreate(index, CreateArgs.builder().withPrefix(index + ":").build(),
				List.of(TagFieldArgs.builder().name("field1").build()));
		redisearch.awaitIndexed(index);
		assertQuery("SELECT count(*) FROM " + index, "VALUES 0");
	}

	@Test
	public void testScanMultiplePages() {
		// 15,000 orders span 15 pages of up to 1024 rows, the last one partial. The expression stops the filter
		// and count(*) from being pushed down, so every row goes through RediSearchPageSource.
		assertQuery("SELECT count(*) FROM orders WHERE custkey * 2 > 0", "VALUES 15000");
	}

	@Test
	public void testScansReadEveryDocument() {
		// Joins and aggregates Trino computes itself see all 15,000 orders; scans used to stop at 10,000 documents
		assertQuery("SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey", "VALUES 15000");
		assertQuery("SELECT count(DISTINCT orderkey) FROM orders", "VALUES 15000");
	}

	@Test
	public void testPushedDownAggregationOverNoDocuments() {
		// With GROUP BY terms there are no groups, so no rows
		assertThat(query("SELECT orderstatus, count(*) FROM orders WHERE totalprice < 0 GROUP BY orderstatus"))
				.isFullyPushedDown().returnsEmptyResult();
		// Without them there is one row: count is 0 and the other aggregates are null
		assertThat(query("SELECT count(*), sum(totalprice), max(totalprice) FROM orders WHERE totalprice < 0"))
				.isFullyPushedDown().matches("VALUES (BIGINT '0', CAST(NULL AS double), CAST(NULL AS double))");
	}

	@Test
	public void testJsonSearch() {
		RedisCommands<String, String> sync = redisearch.getConnection().sync();
		sync.ftCreate("jsontest", CreateArgs.builder().on(TargetType.JSON).build(),
				List.of(TagFieldArgs.builder().name("$.id").as("id").build(),
						TextFieldArgs.builder().name("$.message").as("message").build()));
		redisearch.awaitIndexed("jsontest");
		sync.jsonSet("doc:1", JsonPath.ROOT_PATH, "{\"id\": \"1\", \"message\": \"this is a test\"}");
		sync.jsonSet("doc:2", JsonPath.ROOT_PATH, "{\"id\": \"2\", \"message\": \"this is another test\"}");
		getQueryRunner().execute("select id, message from jsontest");
	}

	@Test
	@Override
	public void testHaving() {
		abort("Not supported by RediSearch connector");
	}

	@Test
	@Override
	public void testShowCreateTable() {
		abort("Not supported by RediSearch connector");
	}

	@Test
	public void testInsertIndex() {
		String index = "insertidx";
		String prefix = index + ":";
		redisearch.getConnection().sync().ftCreate(index, CreateArgs.builder().withPrefix(prefix).build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("name").build()));
		redisearch.awaitIndexed(index);
		assertUpdate(String.format("INSERT INTO %s (id, name) VALUES ('abc', 'mybeer')", index), 1);
		assertThat(query(String.format("SELECT id, name FROM %s", index)))
				.matches("VALUES (VARCHAR 'abc', VARCHAR 'mybeer')");
		List<String> keys = redisearch.getConnection().sync().keys(prefix + "*");
		assertThat(keys).hasSize(1);
		assertThat(keys.get(0)).startsWith(prefix);
	}

	static RuntimeException getTrinoExceptionCause(Throwable e) {
		return Throwables.getCausalChain(e).stream().filter(TestConnectorSmokeTest::isTrinoException).findFirst()
				.map(RuntimeException.class::cast)
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

	@Test
	public void testQueryFailsWhileIndexBuilds() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		String index = "bulkidx";
		redis.eval("for i = 1, 50000 do redis.call('HSET', 'bulk:' .. i, 'id', i) end return 1", ScriptOutputType.INTEGER);
		// FT.CREATE over existing documents indexes them in the background, during which queries see only some of them
		redis.ftCreate(index, CreateArgs.builder().withPrefix("bulk:").build(),
				List.of(NumericFieldArgs.builder().name("id").build()));
		try {
			assertQueryFails("SELECT count(*) FROM " + index,
					"Index bulkidx is still being built \\(\\d+% indexed\\), so its results would be incomplete.*");
			redisearch.awaitIndexed(index);
			assertQuery("SELECT count(*) FROM " + index, "VALUES 50000");
		} finally {
			redis.ftDropindex(index, true);
		}
	}

}
