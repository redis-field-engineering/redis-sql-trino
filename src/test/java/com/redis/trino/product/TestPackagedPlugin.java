package com.redis.trino.product;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;

@ProductTest
@RequiresEnvironment(RediSearchEnvironment.class)
class TestPackagedPlugin {

	@Test
	void testCatalogUsesPlugin(RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = '"
				+ RediSearchEnvironment.CATALOG + "'")).containsOnly(row("redisearch"));
	}

	@Test
	void testSelect(RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT id, name, abv FROM beers ORDER BY id"))
				.containsExactlyInOrder(row("1", "Hocus Pocus", 4.5), row("2", "Grimm's Witbier", 5.0));
	}

	@Test
	void testUnindexedFieldIsQueryable(RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT DISTINCT last_mod FROM beers")).containsOnly(row("2010-07-22 20:00:20 UTC"));
	}

	@Test
	void testPredicatePushdown(RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT name FROM beers WHERE abv > 4.8")).containsOnly(row("Grimm's Witbier"));
		assertThat(env.executeTrino("SELECT name FROM beers WHERE style_name = 'Belgian-Style White'"))
				.containsOnly(row("Grimm's Witbier"));
	}

	@Test
	void testAggregation(RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT count(*) FROM beers")).containsOnly(row(2L));
	}

	@Test
	void testCreateTableAsSelect(RediSearchEnvironment env) {
		String table = "nation_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36);
		try {
			assertThat(env.executeTrinoUpdate(
					"CREATE TABLE " + table + " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation"))
					.isEqualTo(25);
			assertThat(env.executeTrino("SELECT count(*) FROM " + table)).containsOnly(row(25L));
			// NUMERIC fields read back as double
			assertThat(env.executeTrino("SELECT regionkey FROM " + table + " WHERE name = 'CANADA'"))
					.containsOnly(row(1.0));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + table);
		}
	}

}
