package com.redis.trino;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

public final class RediSearchQueryRunner {

	private static final Logger LOG = Logger.get(RediSearchQueryRunner.class);
	private static final String TPCH_SCHEMA = "tpch";

	private RediSearchQueryRunner() {
	}

	public static DistributedQueryRunner createRediSearchQueryRunner(RediSearchServer server, TpchTable<?>... tables)
			throws Exception {
		return createRediSearchQueryRunner(server, ImmutableList.copyOf(tables), ImmutableMap.of(), ImmutableMap.of());
	}

	public static DistributedQueryRunner createRediSearchQueryRunner(RediSearchServer server,
			Iterable<TpchTable<?>> tables, Map<String, String> extraProperties,
			Map<String, String> extraConnectorProperties) throws Exception {
		DistributedQueryRunner queryRunner = null;
		try {
			queryRunner = DistributedQueryRunner.builder(createSession()).setExtraProperties(extraProperties).build();

			queryRunner.installPlugin(new TpchPlugin());
			queryRunner.createCatalog("tpch", "tpch");

			RediSearchConnectorFactory testFactory = new RediSearchConnectorFactory();
			installRediSearchPlugin(server, queryRunner, testFactory, extraConnectorProperties);

			LOG.info("Loading data...");

			long startTime = System.nanoTime();
			for (TpchTable<?> table : tables) {
				loadTpchTopic(server, queryRunner, table);
			}
			LOG.info("Loading complete in %s s", Duration.ofNanos(System.nanoTime() - startTime).toSeconds());
			return queryRunner;
		} catch (Throwable e) {
			closeAllSuppress(e, queryRunner);
			throw e;
		}
	}

	public static <T extends Throwable> T closeAllSuppress(T rootCause, AutoCloseable... closeables) {
		requireNonNull(rootCause, "rootCause is null");
		if (closeables == null) {
			return rootCause;
		}
		for (AutoCloseable closeable : closeables) {
			try {
				if (closeable != null) {
					closeable.close();
				}
			} catch (Throwable e) {
				// Self-suppression not permitted
				if (rootCause != e) {
					rootCause.addSuppressed(e);
				}
			}
		}
		return rootCause;
	}

	private static void installRediSearchPlugin(RediSearchServer server, QueryRunner queryRunner,
			RediSearchConnectorFactory factory, Map<String, String> extraConnectorProperties) {
		queryRunner.installPlugin(new RediSearchPlugin(factory));
		Map<String, String> config = ImmutableMap.<String, String>builder().putAll(server.getConnectorProperties())
				.put("redisearch.default-schema-name", TPCH_SCHEMA)
				.putAll(extraConnectorProperties).buildOrThrow();
		queryRunner.createCatalog("redisearch", "redisearch", config);
	}

	private static void loadTpchTopic(RediSearchServer server, QueryRunner queryRunner, TpchTable<?> table) {
		long start = System.nanoTime();
		LOG.info("Running import for %s", table.getTableName());
		String tableName = table.getTableName().toLowerCase(ENGLISH);
		try (RediSearchLoader loader = new RediSearchLoader(server.getClient(), tableName)) {
			loader.load(queryRunner.execute(
					format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, tableName))));
		}
		LOG.info("Imported %s in %s s", table.getTableName(), Duration.ofNanos(System.nanoTime() - start).toSeconds());
	}

	/**
	 * Asserts that a query returns the rows another one does, in any order, with the same values. QueryAssert's
	 * {@code matches} compares doubles only to 5 significant digits.
	 */
	public static void assertExactRows(QueryRunner queryRunner, String sql, String expectedSql) {
		assertThat(rows(queryRunner, sql)).containsExactlyInAnyOrderElementsOf(rows(queryRunner, expectedSql));
	}

	private static List<List<Object>> rows(QueryRunner queryRunner, String sql) {
		return queryRunner.execute(sql).getMaterializedRows().stream().map(MaterializedRow::getFields).toList();
	}

	public static Session createSession() {
		return testSessionBuilder().setCatalog("redisearch").setSchema(TPCH_SCHEMA).build();
	}

	public static void main(String[] args) throws Exception {
		Logging.initialize();
		DistributedQueryRunner queryRunner = createRediSearchQueryRunner(new RediSearchServer(), TpchTable.getTables(),
				ImmutableMap.of("http-server.http.port", "8080"), ImmutableMap.of());

		Logger log = Logger.get(RediSearchQueryRunner.class);
		log.info("======== SERVER STARTED ========");
		log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
	}
}
