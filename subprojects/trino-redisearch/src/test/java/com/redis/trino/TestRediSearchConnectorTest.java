package com.redis.trino;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.trino.FeaturesConfig.JoinDistributionType;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
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
	public void testLimit() {
		// ignore
	}

	@Override
	public void testLimitMax() {
		// ignore
	}

	@Override
	public void testShowSchemasLikeWithEscape() {
		// ignore
	}

	@Override
	public void testShowTablesLike() {
		// ignore
	}

	@Override
	public void testShowCreateTable() {
		// ignore
	}

	@Override
	public void testLikePredicate() {
		// ignore
	}

	@Override
	public void testJoinWithEmptySides(JoinDistributionType joinDistributionType) {
		// ignore
	}

	@Override
	public void testInformationSchemaFiltering() {
		// ignore
	}

	@Override
	public void testIsNullPredicate() {
		// ignore
	}

	@Override
	public void testShowColumns() {
		// ignore;
	}

	@Override
	protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
		switch (connectorBehavior) {
		case SUPPORTS_ARRAY:
			return false;

		case SUPPORTS_COMMENT_ON_TABLE:
		case SUPPORTS_COMMENT_ON_COLUMN:
			return false;

		case SUPPORTS_CREATE_TABLE:
			return false;

		case SUPPORTS_ADD_COLUMN:
			return false;

		case SUPPORTS_CREATE_VIEW:
		case SUPPORTS_CREATE_SCHEMA:
			return false;

		case SUPPORTS_RENAME_TABLE:
		case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
			return false;

		case SUPPORTS_INSERT:
			return false;

		case SUPPORTS_TOPN_PUSHDOWN:
			return false;
		case SUPPORTS_LIMIT_PUSHDOWN:
			return false;
		case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
			return false;

		default:
			return super.hasBehavior(connectorBehavior);
		}
	}

	/**
	 * This method overrides the default values used for the data provider of the
	 * test {@link AbstractTestQueries#testLargeIn(int)} by taking into account that
	 * by default Elasticsearch supports only up to `1024` clauses in query.
	 * <p>
	 *
	 * @return the amount of clauses to be used in large queries
	 */
    @Override
    protected Object[][] largeInValuesCountData()
    {
        return new Object[][] {
                {200},
                {500},
                {1000}
        };
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

	/**
	 * The column metadata for the Elasticsearch connector tables are provided based
	 * on the column name in alphabetical order.
	 */
	@Test
	@Override
	public void testDescribeTable() {
		MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
				.row("orderkey", "double", "", "")
				.row("custkey", "double", "", "")
				.row("orderstatus", "varchar", "", "")
				.row("totalprice", "double", "", "")
				.row("orderdate", "varchar", "", "")
				.row("orderpriority", "varchar", "", "")
				.row("clerk", "varchar", "", "")
				.row("shippriority", "double", "", "")
				.row("comment", "varchar", "", "").build();
		MaterializedResult actualColumns = computeActual("DESCRIBE orders");
		assertEquals(actualColumns, expectedColumns);
	}

}
