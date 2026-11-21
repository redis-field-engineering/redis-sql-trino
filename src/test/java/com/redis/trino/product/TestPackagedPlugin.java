package com.redis.trino.product;

import static com.redis.trino.product.RediSearchEnvironment.CATALOG;
import static com.redis.trino.product.RediSearchEnvironment.SCHEMA;
import static com.redis.trino.product.RediSearchEnvironment.SHARDED_CATALOG;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;

/**
 * Runs each test on the non-sharded and the sharded database.
 */
@ProductTest
@RequiresEnvironment(RediSearchEnvironment.class)
class TestPackagedPlugin {

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testCatalogUsesPlugin(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino(
				"SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = '" + catalog + "'"))
				.containsOnly(row("redisearch"));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testSelect(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT id, name, abv FROM " + table(catalog, "beers") + " ORDER BY id"))
				.containsExactlyInOrder(row("1", "Hocus Pocus", 4.5), row("2", "Grimm's Witbier", 5.0));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testUnindexedFieldIsQueryable(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT DISTINCT last_mod FROM " + table(catalog, "beers")))
				.containsOnly(row("2010-07-22 20:00:20 UTC"));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testPredicatePushdown(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT name FROM " + table(catalog, "beers") + " WHERE abv > 4.8"))
				.containsOnly(row("Grimm's Witbier"));
		assertThat(env.executeTrino(
				"SELECT name FROM " + table(catalog, "beers") + " WHERE style_name = 'Belgian-Style White'"))
				.containsOnly(row("Grimm's Witbier"));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testAggregation(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT count(*) FROM " + table(catalog, "beers"))).containsOnly(row(2L));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testCreateTableAsSelect(String catalog, RediSearchEnvironment env) {
		String table = table(catalog, "nation_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36));
		try {
			assertThat(env.executeTrinoUpdate(
					"CREATE TABLE " + table + " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation"))
					.isEqualTo(25);
			assertThat(env.executeTrino("SELECT count(*) FROM " + table)).containsOnly(row(25L));
			// The column reads back as the bigint it was created as, not as its NUMERIC field's double
			assertThat(env.executeTrino("SELECT regionkey FROM " + table + " WHERE name = 'CANADA'"))
					.containsOnly(row(1L));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + table);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testMergeAndBooleanValues(String catalog, RediSearchEnvironment env) {
		String table = table(catalog, "merge_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36));
		try {
			assertThat(env.executeTrinoUpdate(
						"CREATE TABLE " + table + " AS SELECT BIGINT '9007199254740993' id, true active"))
					.isEqualTo(1);
			assertThat(env.executeTrinoUpdate("MERGE INTO " + table + " t USING "
					+ "(VALUES (BIGINT '9007199254740993', false), (BIGINT '9007199254740994', true)) s(id, active) "
					+ "ON t.id = s.id WHEN MATCHED THEN UPDATE SET active = s.active "
					+ "WHEN NOT MATCHED THEN INSERT (id, active) VALUES (s.id, s.active)"))
					.isEqualTo(2);
			assertThat(env.executeTrino("SELECT id, active FROM " + table + " ORDER BY id"))
					.containsExactlyInOrder(row(9007199254740993L, false), row(9007199254740994L, true));
			assertThat(env.executeTrinoUpdate("DELETE FROM " + table + " WHERE id = BIGINT '9007199254740993'"))
					.isEqualTo(1);
			assertThat(env.executeTrino("SELECT id, active FROM " + table))
					.containsOnly(row(9007199254740994L, true));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + table);
		}
	}

	private static String table(String catalog, String name) {
		return catalog + "." + SCHEMA + "." + name;
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testNativeSearchLoadsFromPackagedPlugin(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT id, __key FROM TABLE(" + catalog
				+ ".system.search('beers', '@name:pocus'))"))
				.containsOnly(row("1", "beer:1"));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testViewCommentsAndTruncate(String catalog, RediSearchEnvironment env) {
		String suffix = Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36);
		String target = table(catalog, "metadata_" + suffix);
		String view = table(catalog, "view_" + suffix);
		try {
			env.executeTrinoUpdate("CREATE TABLE " + target + " AS SELECT BIGINT '1' id");
			env.executeTrinoUpdate("COMMENT ON TABLE " + target + " IS 'packaged metadata'");
			env.executeTrinoUpdate("CREATE VIEW " + view + " AS SELECT id FROM " + target);
			assertThat(env.executeTrino("SELECT id FROM " + view)).containsOnly(row(1L));
			env.executeTrinoUpdate("TRUNCATE TABLE " + target);
			assertThat(env.executeTrino("SELECT count(*) FROM " + view)).containsOnly(row(0L));
		} finally {
			env.executeTrinoUpdate("DROP VIEW IF EXISTS " + view);
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + target);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testNativeJsonAndEncodedTypes(String catalog, RediSearchEnvironment env) {
		String target = table(catalog, "encoded_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36));
		try {
			env.executeTrinoUpdate("CREATE TABLE " + target + " AS SELECT JSON '{\"a\":1}' payload, "
					+ "ARRAY[BIGINT '9007199254740993', NULL] ids, "
					+ "TIMESTAMP '2026-11-21 12:34:56.123456789012' ts");
			assertThat(env.executeTrino("SELECT json_extract_scalar(payload, '$.a'), CAST(ids[1] AS varchar), "
					+ "CAST(cardinality(ids) AS varchar), CAST(ts AS varchar) FROM " + target))
					.containsOnly(row("1", "9007199254740993", "2", "2026-11-21 12:34:56.123456789012"));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + target);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testStagedDistinctAndTypedTopN(String catalog, RediSearchEnvironment env) {
		String target = table(catalog, "pushdown_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36));
		try {
			env.executeTrinoUpdate("CREATE TABLE " + target + " (g varchar, v varchar, id bigint, amount decimal(20,2))");
			env.executeTrinoUpdate("INSERT INTO " + target + " VALUES "
					+ "('A', 'x', BIGINT '9007199254740993', DECIMAL '12.34'), "
					+ "('A', 'x', BIGINT '9007199254740994', DECIMAL '12.34'), "
					+ "('A', NULL, BIGINT '3', DECIMAL '99.99'), ('B', NULL, BIGINT '4', NULL)");
			assertThat(env.executeTrino("SELECT count(DISTINCT v) FROM " + target)).containsOnly(row(1L));
			assertThat(env.executeTrino("SELECT g, count(DISTINCT v) FROM " + target + " GROUP BY g"))
					.containsOnly(row("A", 1L), row("B", 0L));
			assertThat(env.executeTrino("SELECT id FROM " + target + " ORDER BY amount ASC NULLS FIRST, id DESC LIMIT 3"))
					.containsExactlyInOrder(row(4L), row(9007199254740994L), row(9007199254740993L));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + target);
		}
	}

}
