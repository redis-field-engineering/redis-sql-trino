package com.redis.trino;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.trino.FeaturesConfig.JoinDistributionType;
import io.trino.spi.TrinoException;
import io.trino.sql.parser.ParsingException;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;

public class TestRediSearchConnectorTest extends BaseConnectorTest {

	private RediSearchServer redisearch;

	@Override
	protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
		switch (connectorBehavior) {
		case SUPPORTS_CREATE_SCHEMA:
			return false;

		case SUPPORTS_CREATE_VIEW:
			return false;

		case SUPPORTS_CREATE_TABLE:
			return true;

		case SUPPORTS_RENAME_TABLE:
			return false;

		case SUPPORTS_ARRAY:
			return false;

		case SUPPORTS_DROP_COLUMN:
		case SUPPORTS_RENAME_COLUMN:
			return false;

		case SUPPORTS_COMMENT_ON_TABLE:
		case SUPPORTS_COMMENT_ON_COLUMN:
			return false;

		case SUPPORTS_TOPN_PUSHDOWN:
			return false;

		case SUPPORTS_NOT_NULL_CONSTRAINT:
			return false;

		case SUPPORTS_DELETE:
			return false;

		case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
			return false;

		case SUPPORTS_INSERT:
			return true;

		case SUPPORTS_LIMIT_PUSHDOWN:
			return true;

		case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
			return false;

		default:
			return super.hasBehavior(connectorBehavior);
		}
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = new RediSearchServer();
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, CUSTOMER, NATION, ORDERS, REGION);
	}

	@Override
	protected TestTable createTableWithDefaultColumns() {
		throw new SkipException("test disabled for RediSearch");
	}

	@Override
	public void testShowSchemasLikeWithEscape() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testShowTablesLike() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testShowCreateTable() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testLikePredicate() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testJoinWithEmptySides(JoinDistributionType joinDistributionType) {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testInformationSchemaFiltering() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testIsNullPredicate() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	public void testShowColumns() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Override
	@Test
	public void testLimitMax() {
		int maxLimit = 1000000;
		// max int
		assertQuery("SELECT orderkey FROM orders LIMIT " + maxLimit);
		assertQuery("SELECT orderkey FROM orders ORDER BY orderkey LIMIT " + maxLimit);

		// max long; a connector may attempt a pushdown while remote system may not
		// accept such high limit values
		assertQuery("SELECT nationkey FROM nation LIMIT " + maxLimit, "SELECT nationkey FROM nation");
		// Currently this is not supported but once it's supported, it should be tested
		// with connectors as well
		assertQueryFails("SELECT nationkey FROM nation ORDER BY nationkey LIMIT " + Long.MAX_VALUE,
				"ORDER BY LIMIT > 2147483647 is not supported");
	}

	@SuppressWarnings("resource")
	@Test
	public void testInsert() {
		if (!hasBehavior(SUPPORTS_INSERT)) {
			assertQueryFails("INSERT INTO nation(nationkey) VALUES (42)", "This connector does not support inserts");
			return;
		}

		String query = "SELECT phone, custkey, acctbal FROM customer";

		try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "AS " + query + " WITH NO DATA")) {
			assertQuery("SELECT count(*) FROM " + table.getName() + "", "SELECT 0");

			assertUpdate("INSERT INTO " + table.getName() + " " + query, "SELECT count(*) FROM customer");

			assertQuery("SELECT * FROM " + table.getName() + "", query);

			assertUpdate("INSERT INTO " + table.getName() + " (custkey) VALUES (-1)", 1);
			assertUpdate("INSERT INTO " + table.getName() + " (phone) VALUES ('3283-2001-01-01')", 1);
			assertUpdate("INSERT INTO " + table.getName() + " (custkey, phone) VALUES (-2, '3283-2001-01-02')", 1);
			assertUpdate("INSERT INTO " + table.getName() + " (phone, custkey) VALUES ('3283-2001-01-03', -3)", 1);
			assertUpdate("INSERT INTO " + table.getName() + " (acctbal) VALUES (1234)", 1);

			assertQuery("SELECT * FROM " + table.getName() + "",
					query + " UNION ALL SELECT null, -1, null" + " UNION ALL SELECT '3283-2001-01-01', null, null"
							+ " UNION ALL SELECT '3283-2001-01-02', -2, null"
							+ " UNION ALL SELECT '3283-2001-01-03', -3, null" + " UNION ALL SELECT null, null, 1234");

			// UNION query produces columns in the opposite order
			// of how they are declared in the table schema
			assertUpdate("INSERT INTO " + table.getName() + " (custkey, phone, acctbal) "
					+ "SELECT custkey, phone, acctbal FROM customer " + "UNION ALL "
					+ "SELECT custkey, phone, acctbal FROM customer", "SELECT 2 * count(*) FROM customer");
		}
	}

	/**
	 * @return the amount of clauses to be used in large queries
	 */
	@Override
	protected Object[][] largeInValuesCountData() {
		return new Object[][] { { 200 }, { 500 }, { 1000 } };
	}

	@AfterClass(alwaysRun = true)
	public final void destroy() {
		redisearch.close();
	}

//	@Test
//	@Override
//	public void testSelectAll() {
//		// List columns explicitly, as there's no defined order in RediSearch
//		assertQuery(
//				"SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
//	}

	/**
	 * The column metadata for the RediSearch connector tables are provided based on
	 * the column name in alphabetical order.
	 */
	@Test
	@Override
	public void testDescribeTable() {
		MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
				.row("orderkey", "double", "", "").row("custkey", "double", "", "")
				.row("orderstatus", "varchar", "", "").row("totalprice", "double", "", "")
				.row("orderdate", "varchar", "", "").row("orderpriority", "varchar", "", "")
				.row("clerk", "varchar", "", "").row("shippriority", "double", "", "").row("comment", "varchar", "", "")
				.build();
		MaterializedResult actualColumns = computeActual("DESCRIBE orders");
		assertEquals(actualColumns, expectedColumns);
	}

	@SuppressWarnings("deprecation")
	@Test(dataProvider = "testCaseSensitiveDataMappingProvider", enabled = false)
	public void testCaseSensitiveDataMapping(DataMappingTestSetup dataMappingTestSetup) {
	}

	@SuppressWarnings("deprecation")
	@Test(dataProvider = "testRediSearchDataMappingSmokeTestDataProvider")
	public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup) {
		testDataMapping(dataMappingTestSetup);
	}

	@SuppressWarnings("deprecation")
	private void testDataMapping(DataMappingTestSetup dataMappingTestSetup) {
		skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

		String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
		String sampleValueLiteral = dataMappingTestSetup.getSampleValueLiteral();
		String highValueLiteral = dataMappingTestSetup.getHighValueLiteral();

		String tableName = dataMappingTableName(trinoTypeName);

		Runnable setup = () -> {
			// TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use
			// different connector API methods.
			String createTable = "" + "CREATE TABLE " + tableName + " AS "
					+ "SELECT CAST(row_id AS varchar(50)) row_id, CAST(value AS " + trinoTypeName + ") value "
					+ "FROM (VALUES " + "  ('null value', NULL), " + "  ('sample value', " + sampleValueLiteral + "), "
					+ "  ('high value', " + highValueLiteral + ")) " + " t(row_id, value)";
			assertUpdate(createTable, 3);
		};
		if (dataMappingTestSetup.isUnsupportedType()) {
			String typeNameBase = trinoTypeName.replaceFirst("\\(.*", "");
			String expectedMessagePart = format(
					"(%1$s.*not (yet )?supported)|((?i)unsupported.*%1$s)|((?i)not supported.*%1$s)",
					Pattern.quote(typeNameBase));
			assertThatThrownBy(setup::run).hasMessageFindingMatch(expectedMessagePart)
					.satisfies(e -> assertThat(getTrinoExceptionCause(e)).hasMessageFindingMatch(expectedMessagePart));
			return;
		}
		setup.run();

		// without pushdown, i.e. test read data mapping
		assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + sampleValueLiteral,
				"VALUES 'sample value'");
		assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + highValueLiteral,
				"VALUES 'high value'");

		assertQuery("SELECT row_id FROM " + tableName + " WHERE value = " + sampleValueLiteral,
				"VALUES 'sample value'");
		assertQuery("SELECT row_id FROM " + tableName + " WHERE value != " + sampleValueLiteral, "VALUES 'high value'");
		assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + sampleValueLiteral,
				"VALUES 'sample value'");
		assertQuery("SELECT row_id FROM " + tableName + " WHERE value > " + sampleValueLiteral, "VALUES 'high value'");
		assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + highValueLiteral,
				"VALUES 'sample value', 'high value'");

		assertUpdate("DROP TABLE " + tableName);
	}

	@SuppressWarnings("deprecation")
	@DataProvider
	public final Object[][] testRediSearchDataMappingSmokeTestDataProvider() {
		return testDataMappingSmokeTestData().stream().map(this::filterDataMappingSmokeTestData)
				.flatMap(Optional::stream).collect(toDataProvider());
	}

	@SuppressWarnings("deprecation")
	private List<DataMappingTestSetup> testDataMappingSmokeTestData() {
		return ImmutableList.<DataMappingTestSetup>builder()
//                .add(new DataMappingTestSetup("boolean", "false", "true"))
				.add(new DataMappingTestSetup("tinyint", "37", "127"))
				.add(new DataMappingTestSetup("smallint", "32123", "32767"))
				.add(new DataMappingTestSetup("integer", "1274942432", "2147483647"))
				.add(new DataMappingTestSetup("bigint", "312739231274942432", "9223372036854775807"))
//				  .add(new DataMappingTestSetup("real", "REAL '567.123'", "REAL '999999.999'"))
				.add(new DataMappingTestSetup("double", "DOUBLE '1234567890123.123'", "DOUBLE '9999999999999.999'"))
				.add(new DataMappingTestSetup("decimal(5,3)", "12.345", "99.999"))
				.add(new DataMappingTestSetup("decimal(15,3)", "123456789012.345", "999999999999.99"))
//                .add(new DataMappingTestSetup("date", "DATE '0001-01-01'", "DATE '1582-10-04'")) // before julian->gregorian switch
//                .add(new DataMappingTestSetup("date", "DATE '1582-10-05'", "DATE '1582-10-14'")) // during julian->gregorian switch
//                .add(new DataMappingTestSetup("date", "DATE '2020-02-12'", "DATE '9999-12-31'"))
//                .add(new DataMappingTestSetup("time", "TIME '15:03:00'", "TIME '23:59:59.999'"))
//                .add(new DataMappingTestSetup("timestamp", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999'"))
//                .add(new DataMappingTestSetup("timestamp(3) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999 +12:00'"))
//                .add(new DataMappingTestSetup("char(3)", "'ab'", "'zzz'"))
//                .add(new DataMappingTestSetup("varchar(3)", "'de'", "'zzz'"))
//                .add(new DataMappingTestSetup("varchar", "'łąka for the win'", "'ŻŻŻŻŻŻŻŻŻŻ'"))
//                .add(new DataMappingTestSetup("varbinary", "X'12ab3f'", "X'ffffffffffffffffffff'"))
				.build();
	}

	static RuntimeException getTrinoExceptionCause(Throwable e) {
		return Throwables.getCausalChain(e).stream().filter(TestRediSearchConnectorTest::isTrinoException).findFirst()
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
	public void testInsertHighestUnicodeCharacter() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Test
	public void testInsertUnicode() {
		throw new SkipException("Not supported by RediSearch connector");
	}

	@Test
	public void testCharVarcharComparison() {
		throw new SkipException("Not supported by RediSearch connector");
	}

}
