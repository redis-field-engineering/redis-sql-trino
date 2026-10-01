package com.redis.trino;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

public class TestCaseInsensitiveConnectorSmokeTest extends BaseConnectorSmokeTest {

	private RediSearchServer redisearch;

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = closeAfterClass(new RediSearchServer());
		redisearch.getConnection().sync().flushall();
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch,
				Arrays.asList(CUSTOMER, NATION, ORDERS, REGION), ImmutableMap.of(),
				ImmutableMap.of("redisearch.case-insensitive-names", "true"));
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

	@Test
	@Override
	public void testShowCreateTable() {
		// regionkey is bigint in TPC-H, but NUMERIC fields read back as double
		assertThat((String) computeScalar("SHOW CREATE TABLE region")).isEqualTo(format(
				"CREATE TABLE %s.%s.region (\n" +
						"   regionkey double,\n" +
						"   name varchar,\n" +
						"   comment varchar\n" +
						")",
				getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow()));
	}

	@Test
	public void testMixedCaseIndex() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		String prefix = "beer:";
		redis.hset(prefix + "1", "id", "1");
		redis.hset(prefix + "1", "name", "MyBeer");
		redis.hset(prefix + "2", "id", "2");
		redis.hset(prefix + "2", "name", "MyOtherBeer");
		String index = "MixedCaseBeers";
		redis.ftCreate(index, CreateArgs.builder().withPrefix(prefix).build(),
				List.of(TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("name").build()));
		redisearch.awaitIndexed(index);
		getQueryRunner().execute(MessageFormat.format("SELECT * FROM {0}", index));
		getQueryRunner().execute(MessageFormat.format("SELECT * FROM {0}", index.toLowerCase()));
		getQueryRunner().execute(MessageFormat.format("SELECT * FROM {0}", index.toUpperCase()));
	}

}
