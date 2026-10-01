package com.redis.trino.product;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

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
 * A stock Trino server with the packaged plugin installed, talking to Redis over a shared Docker network.
 * <p>
 * Unlike the smoke tests, which register {@code RediSearchConnectorFactory} in an in-process query runner, this
 * installs the plugin directory produced by {@code mvn package} the way a real deployment does, so it also catches
 * packaging and classloading problems.
 */
public class RediSearchEnvironment extends ProductTestEnvironment {

	public static final String PLUGIN_DIR_PROPERTY = "redisearch.product-tests.plugin-dir";

	public static final String CATALOG = "redisearch";

	public static final String SCHEMA = "default";

	private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:8.4");

	private static final String REDIS_ALIAS = "redis";

	private static final int REDIS_PORT = 6379;

	private Network network;

	private GenericContainer<?> redis;

	private RedisClient client;

	private StatefulRedisConnection<String, String> connection;

	private TrinoContainer trino;

	@Override
	public void start() {
		if (trino != null && trino.isRunning()) {
			return;
		}
		Path pluginDir = pluginDir();

		network = Network.newNetwork();
		redis = new GenericContainer<>(REDIS_IMAGE).withNetwork(network).withNetworkAliases(REDIS_ALIAS)
				.withExposedPorts(REDIS_PORT)
				.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
		redis.start();
		client = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(REDIS_PORT));
		connection = client.connect();
		loadTestData();

		trino = TrinoProductTestContainer.builder().withImage(trinoImage()).withNetwork(network)
				.withCatalog(CATALOG, Map.of("connector.name", "redisearch", "redisearch.uri",
						"redis://" + REDIS_ALIAS + ":" + REDIS_PORT))
				.build();
		trino.withCopyFileToContainer(MountableFile.forHostPath(pluginDir), "/usr/lib/trino/plugin/redisearch");
		TrinoProductTestContainer.startAndWait(trino);
	}

	/**
	 * Direct Redis access for test setup that shouldn't go through the connector.
	 */
	public RedisCommands<String, String> redis() {
		return connection.sync();
	}

	@Override
	protected void afterEachTest() {
		loadTestData();
	}

	// Same shape as the beers fixture in TestConnectorSmokeTest, including an unindexed last_mod field
	private void loadTestData() {
		RedisCommands<String, String> redis = redis();
		redis.flushall();
		redis.ftCreate("beers", CreateArgs.builder().withPrefix("beer:").build(),
				List.of(TagFieldArgs.builder().name("id").build(), TextFieldArgs.builder().name("name").build(),
						NumericFieldArgs.builder().name("abv").build(),
						TagFieldArgs.builder().name("style_name").build()));
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
		if (connection != null) {
			connection.close();
			connection = null;
		}
		if (client != null) {
			client.shutdown();
			client = null;
		}
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
