package com.redis.trino.product;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import com.redis.trino.RediSearchServer;
import com.redis.trino.RedisEnterprise;
import com.redis.trino.RedisEnterprise.Database;
import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.spi.Plugin;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.TrinoTestImages;
import io.trino.testing.containers.environment.ProductTestEnvironment;

/**
 * A stock Trino server with the packaged plugin installed, talking to a Redis Enterprise node over a shared Docker
 * network.
 * <p>
 * Unlike the smoke tests, which register {@code RediSearchConnectorFactory} in an in-process query runner, this
 * installs the plugin directory produced by {@code mvn package} the way a real deployment does, so it also catches
 * packaging and classloading problems.
 * <p>
 * {@link #CATALOG} uses a non-sharded database and {@link #SHARDED_CATALOG} a sharded one, through the connector's
 * cluster client. Both hold the same test data.
 */
public class RediSearchEnvironment extends ProductTestEnvironment {

	public static final String PLUGIN_DIR_PROPERTY = "redisearch.product-tests.plugin-dir";

	public static final String CATALOG = "redisearch";

	public static final String SHARDED_CATALOG = "redisearch_sharded";

	public static final String SCHEMA = "default";

	private static final String REDIS_ALIAS = "redis";

	private Network network;

	private RedisEnterprise redis;

	// Catalog name to its database
	private final Map<String, Database> databases = new LinkedHashMap<>();

	private final List<RedisClient> clients = new ArrayList<>();

	private final Map<String, StatefulRedisConnection<String, String>> connections = new LinkedHashMap<>();

	private TrinoContainer trino;

	@Override
	public void start() {
		if (trino != null && trino.isRunning()) {
			return;
		}
		Path pluginDir = pluginDir();

		network = Network.newNetwork();
		redis = new RedisEnterprise(network, REDIS_ALIAS);
		databases.put(CATALOG, redis.createDatabase(Deployment.NON_SHARDED));
		databases.put(SHARDED_CATALOG, redis.createDatabase(Deployment.SHARDED));
		TrinoProductTestContainer.Builder builder = TrinoProductTestContainer.builder().withImage(trinoImage())
				.withNetwork(network);
		databases.forEach((catalog, database) -> {
			RedisClient client = RedisClient.create(database.getRedisURI());
			clients.add(client);
			connections.put(catalog, client.connect());
			builder.withCatalog(catalog, Map.of("connector.name", "redisearch", "redisearch.uri",
					redis.getNetworkRedisURI(database), "redisearch.cluster",
					String.valueOf(database.getDeployment().isCluster())));
		});
		loadTestData();

		trino = builder.build();
		trino.withCopyFileToContainer(MountableFile.forHostPath(pluginDir), "/usr/lib/trino/plugin/redisearch");
		TrinoProductTestContainer.startAndWait(trino);
	}

	/**
	 * Direct access to a catalog's database, for test setup that shouldn't go through the connector.
	 */
	public RedisCommands<String, String> redis(String catalog) {
		return connections.get(catalog).sync();
	}

	@Override
	protected void afterEachTest() {
		loadTestData();
	}

	private void loadTestData() {
		connections.keySet().forEach(this::loadTestData);
	}

	// Same shape as the beers fixture in TestConnectorSmokeTest, including an unindexed last_mod field
	private void loadTestData(String catalog) {
		RedisCommands<String, String> redis = redis(catalog);
		redis.flushall();
		redis.ftCreate("beers", CreateArgs.builder().withPrefix("beer:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TextFieldArgs.builder().name("name").build(),
						NumericFieldArgs.builder().name("abv").build(),
						TagFieldArgs.builder().name("style_name").build()));
		RediSearchServer.awaitIndexed(redis, "beers");
		redis.hset("beer:1", Map.of("id", "1", "name", "Hocus Pocus", "abv", "4.5", "style_name",
				"Light American Wheat Ale or Lager", "last_mod", "2010-07-22 20:00:20 UTC"));
		redis.hset("beer:2", Map.of("id", "2", "name", "Grimm's Witbier", "abv", "5.0", "style_name",
				"Belgian-Style White", "last_mod", "2010-07-22 20:00:20 UTC"));
	}

	@Override
	public Connection createTrinoConnection() throws SQLException {
		return createTrinoConnection("test");
	}

	@Override
	public Connection createTrinoConnection(String user) throws SQLException {
		return TrinoProductTestContainer.createConnection(trino, user, CATALOG, SCHEMA);
	}

	@Override
	public String getTrinoJdbcUrl() {
		return trino.getJdbcUrl();
	}

	@Override
	public boolean isRunning() {
		return trino != null && trino.isRunning();
	}

	@Override
	protected void doClose() {
		if (trino != null) {
			trino.close();
			trino = null;
		}
		connections.values().forEach(StatefulRedisConnection::close);
		connections.clear();
		clients.forEach(RedisClient::shutdown);
		clients.clear();
		databases.clear();
		if (redis != null) {
			redis.close();
			redis = null;
		}
		if (network != null) {
			network.close();
			network = null;
		}
	}

	private static Path pluginDir() {
		String dir = System.getProperty(PLUGIN_DIR_PROPERTY);
		if (dir == null || !Files.isDirectory(Path.of(dir))) {
			throw new IllegalStateException(format(
					"Packaged plugin directory not found (%s=%s). Run ./mvnw verify -Pproduct-tests, or run "
							+ "./mvnw package and pass -D%s=target/redis-sql-trino-<version>",
					PLUGIN_DIR_PROPERTY, dir, PLUGIN_DIR_PROPERTY));
		}
		return Path.of(dir);
	}

	// The framework defaults to trinodb/trino:latest; match the SPI version the plugin was compiled against instead
	private static String trinoImage() {
		String spiVersion = requireNonNull(Plugin.class.getPackage().getImplementationVersion(),
				"trino-spi Implementation-Version is missing");
		return System.getProperty(TrinoTestImages.TRINO_IMAGE_PROPERTY, "trinodb/trino:" + spiVersion);
	}

}
