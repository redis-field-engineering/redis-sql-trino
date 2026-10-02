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

	private static String table(String catalog, String name) {
		return catalog + "." + SCHEMA + "." + name;
	}
}
