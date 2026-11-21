package com.redis.trino;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
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
		case SUPPORTS_CREATE_MATERIALIZED_VIEW:
			return false;

		case SUPPORTS_CREATE_VIEW:
		case SUPPORTS_CREATE_TABLE:
			return true;

		case SUPPORTS_ARRAY:
			return true;

		case SUPPORTS_DROP_COLUMN:
		case SUPPORTS_RENAME_COLUMN:
		case SUPPORTS_RENAME_TABLE:
			return false;

		case SUPPORTS_COMMENT_ON_TABLE:
		case SUPPORTS_COMMENT_ON_COLUMN:
			return true;

		case SUPPORTS_TOPN_PUSHDOWN:
			// Optimization is limited by type, number of sort keys and limit; Trino retains the final sort.
			// Dedicated tests cover the supported server and late-materialization paths.
			return false;

		case SUPPORTS_NOT_NULL_CONSTRAINT:
			return false;

		case SUPPORTS_DELETE:
		case SUPPORTS_INSERT:
		case SUPPORTS_UPDATE:
		case SUPPORTS_MERGE:
			return true;

		case SUPPORTS_TRUNCATE:
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
	public void testRestrictedTopNWithCaseInsensitiveNames() {
		var redis = redisearch.getConnection().sync();
		String index = "MixedCaseRanks";
		redis.ftCreate(index, CreateArgs.builder().withPrefix("mixed_rank:").build(), List.of(
				TagFieldArgs.builder().name("id").build(), NumericFieldArgs.builder().name("score").build()));
		redis.hset("mixed_rank:1", Map.of("id", "z", "score", "1"));
		redis.hset("mixed_rank:2", Map.of("id", "b", "score", "2"));
		redis.hset("mixed_rank:3", Map.of("id", "a", "score", "3"));
		redis.hset("mixed_rank:4", Map.of("id", "missing"));
		redisearch.awaitIndexed(index);
		try {
			var numeric = getDistributedQueryRunner().executeWithPlan(getSession(),
					"SELECT id FROM MIXEDCASERANKS ORDER BY score DESC LIMIT 2");
			assertThat(numeric.result().getOnlyColumn()).containsExactly("a", "b");
			assertThat(getDistributedQueryRunner().getCoordinator().getQueryManager()
					.getFullQueryInfo(numeric.queryId()).getQueryStats().getPhysicalInputPositions()).isEqualTo(2);
			// Exact local TopN scans all four rows and returns only the winner to Trino's final sort.
			var strings = getDistributedQueryRunner().executeWithPlan(getSession(),
					"SELECT id FROM mixedcaseranks ORDER BY id, score LIMIT 1");
			assertThat(strings.result().getOnlyValue()).isEqualTo("a");
			assertLocalTopNReads(strings);
			var nullsFirst = getDistributedQueryRunner().executeWithPlan(getSession(),
					"SELECT id FROM mixedcaseranks ORDER BY score NULLS FIRST LIMIT 1");
			assertThat(nullsFirst.result().getOnlyValue()).isEqualTo("missing");
			assertLocalTopNReads(nullsFirst);
		} finally {
			redis.ftDropindex(index, true);
		}
	}

	private void assertLocalTopNReads(MaterializedResultWithPlan result) {
		var stats = getDistributedQueryRunner().getCoordinator().getQueryManager()
				.getFullQueryInfo(result.queryId()).getQueryStats();
		assertThat(stats.getPhysicalInputPositions()).isEqualTo(1);
		var metrics = stats.getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
				.reduce(Metrics.EMPTY, Metrics::mergeWith).getMetrics();
		assertThat(((LongCount) metrics.get("redis.rows.received")).getTotal()).isEqualTo(4);
		assertThat(((LongCount) metrics.get("redis.local-topn.candidates")).getTotal()).isEqualTo(1);
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
