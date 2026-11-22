package com.redis.trino.product;

import static com.redis.trino.product.RediSearchEnvironment.CATALOG;
import static com.redis.trino.product.RediSearchEnvironment.SCHEMA;
import static com.redis.trino.product.RediSearchEnvironment.SHARDED_CATALOG;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.redis.trino.RediSearchServer;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.VectorFieldArgs;

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
	void testExplainShowsPushdownPolicy(String catalog, RediSearchEnvironment env) {
		String plan = (String) env.executeTrino("EXPLAIN SELECT count(*) FROM " + table(catalog, "beers")).getOnlyValue();
		assertThat(plan).contains("pushdownPolicy", "aggregation=enabled_subject_to_expression_safety", "appliedPushdowns");
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
	void testArithmeticCountsAndMultiWildcardLike(String catalog, RediSearchEnvironment env) {
		String target = table(catalog, "expression_count_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36));
		try {
			env.executeTrinoUpdate("CREATE TABLE " + target + " (g varchar, d double, e double, value varchar)");
			env.executeTrinoUpdate("INSERT INTO " + target + " VALUES "
					+ "('a', 0e0, 0e0, 'pre-one-mid-two-suf'), ('a', 2e0, 0e0, 'pre-one-mid-three-suf'), "
					+ "('b', 4e0, 2e0, 'unrelated'), ('b', NULL, 3e0, NULL), ('null', NULL, NULL, 'presuf')");
			assertThat(env.executeTrino("SELECT count(d + e), count(d / e), sum(d + e), count(*) FROM " + target))
					.containsOnly(row(3L, 3L, 8.0, 5L));
			assertThat(env.executeTrino("SELECT g, count(d / e) FROM " + target + " GROUP BY g"))
					.containsOnly(row("a", 2L), row("b", 1L), row("null", 0L));
			assertThat(env.executeTrino("SELECT count(d + e) FROM " + target + " WHERE g = 'missing'"))
					.containsOnly(row(0L));
			assertThat(env.executeTrino("SELECT value FROM " + target + " WHERE value LIKE 'pre%one%mid%two%suf'"))
					.containsOnly(row("pre-one-mid-two-suf"));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + target);
		}
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
	void testNativeSearchBindsNumericParameters(String catalog, RediSearchEnvironment env) {
		assertThat(env.executeTrino("SELECT id FROM TABLE(" + catalog
				+ ".system.search(TABLE_NAME => 'beers', QUERY => '@abv:[$min +inf]', "
				+ "PARAMETERS => MAP(ARRAY['min'], ARRAY[to_utf8('4.8')])))"))
				.containsOnly(row("2"));
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testNativeVectorParametersPreserveKnnMembership(String catalog, RediSearchEnvironment env) {
		String index = "vectors_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36);
		var redis = env.redis(catalog);
		redis.ftCreate(index, CreateArgs.builder().withPrefix(index + ":").skipInitialScan().build(), List.of(
				TagFieldArgs.builder().name("id").build(),
				VectorFieldArgs.builder().name("vec").flat().type(VectorFieldArgs.VectorType.FLOAT32)
						.dimensions(2).distanceMetric(VectorFieldArgs.DistanceMetric.L2).build()));
		try {
			String[] ids = {"nearest", "second", "far"};
			float[] coordinates = {0, 1, 10};
			for (int i = 0; i < ids.length; i++) {
				byte[] vector = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
						.putFloat(coordinates[i]).putFloat(0).array();
				redis.dispatch(CommandType.HSET, new IntegerOutput<>(StringCodec.UTF8),
						new CommandArgs<>(StringCodec.UTF8).addKey(index + ":" + i)
								.add("id").addValue(ids[i]).add("vec").add(vector));
			}
			String source = "TABLE(" + catalog + ".system.search(TABLE_NAME => '" + index
					+ "', QUERY => '*=>[KNN 2 @vec $blob]', PARAMETERS => "
					+ "MAP(ARRAY['blob'], ARRAY[from_hex('0000000000000000')])))";
			assertThat(env.executeTrino("SELECT id FROM " + source)).containsOnly(row("nearest"), row("second"));
			assertThat(env.executeTrino("SELECT count(*) FROM " + source)).containsOnly(row(2L));
			// Filtering the selected KNN rowset must not replace a neighbor with a farther matching document.
			assertThat(env.executeTrino("SELECT count(*) FROM " + source + " WHERE id = 'far'")).containsOnly(row(0L));
			assertThat(env.executeTrino("SELECT id FROM " + source + " ORDER BY id DESC LIMIT 1"))
					.containsOnly(row("second"));
		} finally {
			redis.ftDropindex(index, true);
		}
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

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testAnchoredLikeAndEscapedLiterals(String catalog, RediSearchEnvironment env) {
		String target = table(catalog, "like_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36));
		try {
			env.executeTrinoUpdate("CREATE TABLE " + target + " (value varchar)");
			env.executeTrinoUpdate("INSERT INTO " + target + " VALUES ('pre-middle-suf'), ('presuf'), "
					+ "('pre%suf'), ('pre_suf'), ('aba'), ('abaaba'), (NULL)");
			assertThat(env.executeTrino("SELECT value FROM " + target + " WHERE value LIKE 'pre%suf'"))
					.containsOnly(row("pre-middle-suf"), row("presuf"), row("pre%suf"), row("pre_suf"));
			assertThat(env.executeTrino("SELECT value FROM " + target + " WHERE value LIKE 'pre!%suf' ESCAPE '!'"))
					.containsOnly(row("pre%suf"));
			assertThat(env.executeTrino("SELECT value FROM " + target + " WHERE value LIKE 'pre!_suf' ESCAPE '!'"))
					.containsOnly(row("pre_suf"));
			assertThat(env.executeTrino("SELECT value FROM " + target + " WHERE value LIKE 'aba%aba'"))
					.containsOnly(row("abaaba"));
		} finally {
			env.executeTrinoUpdate("DROP TABLE IF EXISTS " + target);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testJsonTopNUsesNestedAliasesAndExactFilters(String catalog, RediSearchEnvironment env) {
		String index = "json_topn_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36);
		String target = table(catalog, index);
		var redis = env.redis(catalog);
		redis.ftCreate(index, CreateArgs.builder().on(CreateArgs.TargetType.JSON).withPrefix(index + ":").build(),
				List.of(TagFieldArgs.builder().name("$.doc.id").as("id").build(),
						NumericFieldArgs.builder().name("$.doc.rank").as("rank").build(),
						TagFieldArgs.builder().name("$.doc.payload").as("payload").build()));
		try {
			RediSearchServer.awaitIndexed(redis, index);
			redis.jsonSet(index + ":1", JsonPath.ROOT_PATH, "{\"doc\":{\"id\":\"z\",\"rank\":2,\"payload\":\"last\"}}");
			redis.jsonSet(index + ":2", JsonPath.ROOT_PATH, "{\"doc\":{\"id\":\"b\",\"rank\":3,\"payload\":\"rank first\"}}");
			redis.jsonSet(index + ":3", JsonPath.ROOT_PATH, "{\"doc\":{\"id\":\"a\",\"rank\":3,\"payload\":\"tie first\"}}");
			redis.jsonSet(index + ":4", JsonPath.ROOT_PATH, "{\"doc\":{\"id\":\"n\",\"payload\":\"null first\"}}");
			redis.jsonSet(index + ":5", JsonPath.ROOT_PATH, "{\"doc\":{\"id\":\"A\",\"rank\":1000,\"payload\":\"upper\"}}");
			assertThat(env.executeTrino("SELECT id, payload FROM " + target
					+ " ORDER BY rank DESC NULLS FIRST, id ASC LIMIT 3"))
					.containsExactlyInOrder(row("n", "null first"), row("A", "upper"), row("a", "tie first"));
			assertThat(env.executeTrino("SELECT id FROM " + target + " WHERE id = 'a' ORDER BY rank DESC LIMIT 1"))
					.containsOnly(row("a"));
		} finally {
			redis.ftDropindex(index, true);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { CATALOG, SHARDED_CATALOG })
	void testJsonTopNWithQuotedAndWildcardPaths(String catalog, RediSearchEnvironment env) {
		String index = "json_paths_" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36);
		var redis = env.redis(catalog);
		redis.ftCreate(index, CreateArgs.builder().on(CreateArgs.TargetType.JSON)
				.withPrefix(index + ":").skipInitialScan().build(), List.of(
				TagFieldArgs.builder().name("$.id").as("id").build(),
				NumericFieldArgs.builder().name("$['odd.key'][*]").as("rank").build(),
				TagFieldArgs.builder().name("$['雪 key']").as("label").build()));
		try {
			redis.jsonSet(index + ":a", JsonPath.ROOT_PATH, "{\"id\":\"a\",\"odd.key\":[2,99],\"雪 key\":\"first\"}");
			redis.jsonSet(index + ":b", JsonPath.ROOT_PATH, "{\"id\":\"b\",\"odd.key\":[1,88],\"雪 key\":\"second\"}");
			redis.jsonSet(index + ":c", JsonPath.ROOT_PATH, "{\"id\":\"c\",\"odd.key\":[3,0],\"雪 key\":\"third\"}");
			assertThat(env.executeTrino("SELECT id, label FROM " + table(catalog, index) + " ORDER BY rank ASC LIMIT 2"))
					.containsExactlyInOrder(row("b", "second"), row("a", "first"));
			// Numeric index matches on later array values are only a prefilter for SQL's first-value semantics.
			assertThat(env.executeTrino("SELECT count(*) FROM " + table(catalog, index) + " WHERE rank > 50"))
					.containsOnly(row(0L));
			assertThat(env.executeTrino("SELECT id FROM " + table(catalog, index) + " WHERE rank > 1 ORDER BY rank ASC LIMIT 1"))
					.containsOnly(row("a"));
		} finally {
			redis.ftDropindex(index, true);
		}
	}

}
