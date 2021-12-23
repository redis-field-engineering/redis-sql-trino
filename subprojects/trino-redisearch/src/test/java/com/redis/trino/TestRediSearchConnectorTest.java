package com.redis.trino;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

public class TestRediSearchConnectorTest extends BaseConnectorTest {

	private RediSearchServer redisearch;

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = new RediSearchServer();
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, CUSTOMER, NATION, ORDERS, REGION);
	}

	@Override
	protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
		switch (connectorBehavior) {
		case SUPPORTS_COMMENT_ON_TABLE:
		case SUPPORTS_COMMENT_ON_COLUMN:
			return false;

		case SUPPORTS_CREATE_TABLE:
			return false;

		case SUPPORTS_CREATE_SCHEMA:
			return false;

		case SUPPORTS_RENAME_TABLE:
		case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
			return false;

		case SUPPORTS_INSERT:
			return false;

		case SUPPORTS_LIMIT_PUSHDOWN:
		case SUPPORTS_TOPN_PUSHDOWN:
			return false;

		default:
			return super.hasBehavior(connectorBehavior);
		}
	}

	@AfterClass(alwaysRun = true)
	public final void destroy() {
		redisearch.close();
	}

	@Test
	@Override
	public void testSelectAll() {
		// List columns explicitly, as there's no defined order in Elasticsearch
		assertQuery(
				"SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
	}
}
