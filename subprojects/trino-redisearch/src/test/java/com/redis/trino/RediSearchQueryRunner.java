package com.redis.trino;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.redis.lettucemod.test.Beers;

import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

public final class RediSearchQueryRunner {

	private static final Logger LOG = Logger.get(RediSearchQueryRunner.class);
	private static final String TPCH_SCHEMA = "tpch";

	private RediSearchQueryRunner() {
	}

	public static DistributedQueryRunner createRediSearchQueryRunner(RediSearchServer server, TpchTable<?>... tables)
			throws Exception {
		return createRediSearchQueryRunner(server, ImmutableMap.of(), ImmutableList.copyOf(tables));
	}

	public static DistributedQueryRunner createRediSearchQueryRunner(RediSearchServer server,
			Map<String, String> extraProperties, Iterable<TpchTable<?>> tables) throws Exception {
		DistributedQueryRunner queryRunner = null;
		try {
			queryRunner = DistributedQueryRunner.builder(createSession()).setExtraProperties(extraProperties).build();

			queryRunner.installPlugin(new TpchPlugin());
			queryRunner.createCatalog("tpch", "tpch");

			Map<String, String> properties = ImmutableMap.<String, String>builder()
					.put("redisearch.uri", server.getTestContext().getRedisURI())
					.put("redisearch.default-schema-name", TPCH_SCHEMA).build();

			queryRunner.installPlugin(new RediSearchPlugin());
			queryRunner.createCatalog("redisearch", "redisearch", properties);

			TestingTrinoClient trinoClient = queryRunner.getClient();

			LOG.info("Loading data...");

			long startTime = System.nanoTime();
			Beers.populateIndex(server.getTestContext().getConnection());
//			for (TpchTable<?> table : tables) {
//				loadTpchTopic(server, trinoClient, table);
//			}
			LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
			return queryRunner;
		} catch (Throwable e) {
			closeAllSuppress(e, queryRunner);
			throw e;
		}
	}

	private static void loadTpchTopic(RediSearchServer server, TestingTrinoClient trinoClient, TpchTable<?> table) {
		long start = System.nanoTime();
		LOG.info("Running import for %s", table.getTableName());
		RediSearchLoader loader = new RediSearchLoader(server.getTestContext(),
				table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession());
		loader.execute(format("SELECT * from %s",
				new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
		LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
	}

	public static Session createSession() {
		return testSessionBuilder().setCatalog("redisearch").setSchema(TPCH_SCHEMA).build();
	}

	public static void main(String[] args) throws Exception {
		Logging.initialize();
		DistributedQueryRunner queryRunner = createRediSearchQueryRunner(new RediSearchServer(),
				ImmutableMap.of("http-server.http.port", "8080"), TpchTable.getTables());
		Thread.sleep(10);
		Logger log = Logger.get(RediSearchQueryRunner.class);
		log.info("======== SERVER STARTED ========");
		log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
	}
}
