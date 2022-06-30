package com.redis.trino;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.testing.Closeables;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
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

			TestingTrinoClient trinoClient = queryRunner.getClient();

			LOG.info("Loading data...");

			long startTime = System.nanoTime();
			for (TpchTable<?> table : tables) {
				loadTpchTopic(server, trinoClient, table);
			}
			LOG.info("Loading complete in %s", Duration.nanosSince(startTime).toString(SECONDS));
			return queryRunner;
		} catch (Throwable e) {
			Closeables.closeAllSuppress(e, queryRunner);
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
		Map<String, String> config = ImmutableMap.<String, String>builder()
				.put("redisearch.uri", server.getTestContext().getRedisURI()).put("redisearch.default-limit", "100000")
				.put("redisearch.default-schema-name", TPCH_SCHEMA).putAll(extraConnectorProperties).build();
		queryRunner.createCatalog("redisearch", "redisearch", config);
	}

	private static void loadTpchTopic(RediSearchServer server, TestingTrinoClient trinoClient, TpchTable<?> table) {
		long start = System.nanoTime();
		LOG.info("Running import for %s", table.getTableName());
		RediSearchLoader loader = new RediSearchLoader(server.getTestContext(),
				table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession());
		loader.execute(format("SELECT * from %s",
				new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
		LOG.info("Imported %s in %s", table.getTableName(), Duration.nanosSince(start).convertToMostSuccinctTimeUnit());
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
