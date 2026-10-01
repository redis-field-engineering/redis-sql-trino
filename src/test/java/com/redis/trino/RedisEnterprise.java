package com.redis.trino;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.Capability;
import com.google.common.collect.ImmutableMap;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.testing.containers.junit.ReportLeakedContainers;

/**
 * A single-node Redis Enterprise Software cluster in a container, on which tests create and delete databases.
 * <p>
 * A node takes about 20 seconds to bootstrap and uses over 1 GB of memory, so test classes share one cluster per JVM
 * ({@link #shared()}) and each creates its own database. The trial license allows 4 shards in total, so
 * {@link #createDatabase} waits while other test classes hold them.
 */
public class RedisEnterprise implements Closeable {

	/**
	 * The database topologies the connector supports.
	 */
	public enum Deployment {
		/**
		 * One shard, which clients reach through the database endpoint (redisearch.cluster=false).
		 */
		NON_SHARDED(1),
		/**
		 * Two shards behind the OSS Cluster API, which clients reach through the cluster topology
		 * (redisearch.cluster=true).
		 */
		SHARDED(2);

		private final int shards;

		Deployment(int shards) {
			this.shards = shards;
		}

		public int getShards() {
			return shards;
		}

		public boolean isCluster() {
			return this == SHARDED;
		}
	}

	/**
	 * A database on the cluster, reachable from the test JVM at {@link #getHost()}:{@link #getPort()}.
	 */
	public static class Database {
		private final long uid;
		private final String host;
		private final int port;
		private final Deployment deployment;

		private Database(long uid, String host, int port, Deployment deployment) {
			this.uid = uid;
			this.host = host;
			this.port = port;
			this.deployment = deployment;
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}

		public Deployment getDeployment() {
			return deployment;
		}

		public String getRedisURI() {
			return "redis://" + host + ":" + port;
		}
	}

	// The latest Redis Enterprise Software release; databases use the newest Redis version it supports
	public static final DockerImageName IMAGE = DockerImageName.parse("redislabs/redis:8.2.0-78.18");

	private static final Logger log = Logger.get(RedisEnterprise.class);

	private static final int API_PORT = 9443;
	private static final int LICENSE_SHARDS = 4;
	// Database ports must be in the range Redis Enterprise reserves for them
	private static final int MIN_DATABASE_PORT = 10000;
	private static final int MAX_DATABASE_PORT = 19999;
	private static final int DATABASE_PORTS = 8;
	private static final long DATABASE_MEMORY = 256L * 1024 * 1024;
	private static final Duration TIMEOUT = Duration.ofMinutes(3);
	private static final String USERNAME = "admin@redis.test";

	private static final JsonCodec<Map<String, Object>> JSON = JsonCodec.mapJsonCodec(String.class, Object.class);
	private static final JsonCodec<Object> JSON_BODY = JsonCodec.jsonCodec(Object.class);

	private static RedisEnterprise shared;

	private final Optional<String> networkAlias;
	private final String password = UUID.randomUUID().toString();
	private final GenericContainer<?> container;
	private final HttpClient http;
	private final Semaphore shards = new Semaphore(LICENSE_SHARDS, true);
	private final Deque<Integer> freePorts = new ArrayDeque<>();
	private final AtomicInteger databaseCount = new AtomicInteger();
	private String redisVersion;

	/**
	 * @return the cluster shared by every test in this JVM, started on first use. Testcontainers removes it when the
	 *         JVM exits.
	 */
	public static synchronized RedisEnterprise shared() {
		if (shared == null) {
			shared = new RedisEnterprise(Optional.empty());
			ReportLeakedContainers.ignoreContainerId(shared.container.getContainerId());
		}
		return shared;
	}

	/**
	 * A cluster on {@code network} under {@code networkAlias}, for clients in other containers. Sharded databases
	 * advertise the node's address on that network to cluster clients, instead of the host's.
	 */
	public RedisEnterprise(Network network, String networkAlias) {
		this(Optional.of(new Placement(network, networkAlias)));
	}

	private record Placement(Network network, String alias) {
	}

	// Sharded databases tell cluster clients to connect to <address>:<database port>, so each database port is
	// published on the same host port
	private static class NodeContainer extends GenericContainer<NodeContainer> {
		NodeContainer(List<Integer> databasePorts) {
			super(IMAGE);
			databasePorts.forEach(port -> addFixedExposedPort(port, port));
		}
	}

	private RedisEnterprise(Optional<Placement> placement) {
		this.networkAlias = placement.map(Placement::alias);
		List<Integer> ports = freeHostPorts();
		freePorts.addAll(ports);
		NodeContainer node = new NodeContainer(ports);
		node.withExposedPorts(API_PORT)
				.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withCapAdd(Capability.SYS_RESOURCE))
				.waitingFor(Wait.forHttps("/v1/bootstrap").forPort(API_PORT).allowInsecure().forStatusCode(200)
						.withStartupTimeout(TIMEOUT));
		placement.ifPresent(p -> node.withNetwork(p.network()).withNetworkAliases(p.alias()));
		this.container = node;
		this.http = HttpClient.newBuilder().sslContext(trustAll()).connectTimeout(Duration.ofSeconds(10)).build();
		long start = System.nanoTime();
		container.start();
		try {
			bootstrap(placement.isEmpty());
		} catch (RuntimeException e) {
			container.close();
			throw e;
		}
		log.info("Started Redis Enterprise %s in %s s", IMAGE.getVersionPart(),
				Duration.ofNanos(System.nanoTime() - start).toSeconds());
	}

	private void bootstrap(boolean advertiseHost) {
		request("POST", "/v1/bootstrap/create_cluster", ImmutableMap.of("action", "create_cluster", "cluster",
				Map.of("name", "cluster.local"), "node",
				Map.of("paths", Map.of("persistent_path", "/var/opt/redislabs/persist", "ephemeral_path",
						"/var/opt/redislabs/tmp")),
				"credentials", Map.of("username", USERNAME, "password", password)), false);
		await("cluster bootstrap", () -> {
			HttpResponse<String> response = send("GET", "/v1/bootstrap", null, false);
			if (response.statusCode() == 401) {
				// Once the cluster exists, the API requires its credentials
				response = send("GET", "/v1/bootstrap", null, true);
			}
			if (response.statusCode() != 200) {
				return false;
			}
			Map<String, Object> status = map(JSON.fromJson(response.body()).get("bootstrap_status"));
			if ("error".equals(status.get("state"))) {
				throw new IllegalStateException("Redis Enterprise bootstrap failed: " + status);
			}
			return "completed".equals(status.get("state"));
		});
		await("REST API", () -> send("GET", "/v1/nodes/1", null, true).statusCode() == 200);
		Map<String, Object> node = request("GET", "/v1/nodes/1", null, true);
		// The newest Redis version the cluster can run
		redisVersion = list(node.get("supported_database_versions")).stream().map(RedisEnterprise::map)
				.filter(version -> "redis".equals(version.get("db_type"))).map(version -> (String) version.get("redis_version"))
				.max(Comparator.comparing(version -> List.of(version.split("\\.")).stream().map(Integer::parseInt).toList(),
						RedisEnterprise::compareVersions))
				.orElseThrow();
		if (advertiseHost) {
			request("PUT", "/v1/nodes/1", Map.of("external_addr", List.of(hostAddress())), true);
		}
	}

	private static int compareVersions(List<Integer> left, List<Integer> right) {
		for (int i = 0; i < Math.min(left.size(), right.size()); i++) {
			int result = Integer.compare(left.get(i), right.get(i));
			if (result != 0) {
				return result;
			}
		}
		return Integer.compare(left.size(), right.size());
	}

	/**
	 * @return the Redis version databases run, e.g. 8.6
	 */
	public String getRedisVersion() {
		return redisVersion;
	}

	/**
	 * Creates a database with the Query Engine and JSON, waiting for free license shards if needed.
	 */
	public Database createDatabase(Deployment deployment) {
		acquireShards(deployment.getShards());
		int port;
		synchronized (freePorts) {
			port = requireNonNull(freePorts.poll(), "No free database port");
		}
		try {
			ImmutableMap.Builder<String, Object> spec = ImmutableMap.<String, Object>builder()
					.put("name", "db" + databaseCount.incrementAndGet()).put("type", "redis")
					.put("memory_size", DATABASE_MEMORY).put("port", port).put("redis_version", redisVersion)
					.put("module_list", List.of(Map.of("module_name", "search", "module_args", ""),
							Map.of("module_name", "ReJSON", "module_args", "")));
			if (deployment == Deployment.SHARDED) {
				spec.put("sharding", true).put("shards_count", deployment.getShards()).put("oss_cluster", true)
						.put("oss_cluster_api_preferred_ip_type", networkAlias.isPresent() ? "internal" : "external")
						.put("proxy_policy", "all-master-shards")
						.put("shard_key_regex", List.of(Map.of("regex", ".*\\{(?<tag>.*)\\}.*"), Map.of("regex", "(?<tag>.*)")));
			}
			long uid = create(spec.buildOrThrow());
			await("database " + uid, () -> "active".equals(request("GET", "/v1/bdbs/" + uid, null, true).get("status")));
			String host = container.getHost();
			await("database " + uid + " endpoint", () -> isPingable(host, port));
			return new Database(uid, host, port, deployment);
		} catch (RuntimeException e) {
			release(port, deployment);
			throw e;
		}
	}

	// The node also refuses a database it has no memory left for, until other test classes delete theirs
	private long create(Map<String, Object> spec) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			HttpResponse<String> response = send("POST", "/v1/bdbs", spec, true);
			if (response.statusCode() / 100 == 2) {
				return ((Number) JSON.fromJson(response.body()).get("uid")).longValue();
			}
			if (!response.body().contains("insufficient_resources") || System.nanoTime() > deadline) {
				throw new IllegalStateException(
						format("POST /v1/bdbs returned %s: %s", response.statusCode(), response.body()));
			}
			sleep(Duration.ofSeconds(1));
		}
	}

	public void deleteDatabase(Database database) {
		send("DELETE", "/v1/bdbs/" + database.uid, null, true);
		// The license counts shards until the database is gone
		await("deleting database " + database.uid,
				() -> send("GET", "/v1/bdbs/" + database.uid, null, true).statusCode() == 404);
		release(database.port, database.deployment);
	}

	private void release(int port, Deployment deployment) {
		synchronized (freePorts) {
			freePorts.add(port);
		}
		shards.release(deployment.getShards());
	}

	/**
	 * @return the host and port other containers on the network reach a database at
	 */
	public String getNetworkRedisURI(Database database) {
		return "redis://" + networkAlias.orElseThrow() + ":" + database.port;
	}

	// Test classes run on JUnit's fork-join pool, which adds a thread while this one waits for shards
	private void acquireShards(int count) {
		try {
			ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
				private boolean acquired;

				@Override
				public boolean block() throws InterruptedException {
					shards.acquire(count);
					acquired = true;
					return true;
				}

				@Override
				public boolean isReleasable() {
					if (!acquired) {
						acquired = shards.tryAcquire(count);
					}
					return acquired;
				}
			});
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	private Map<String, Object> request(String method, String path, Object body, boolean authenticate) {
		HttpResponse<String> response = send(method, path, body, authenticate);
		if (response.statusCode() / 100 != 2) {
			throw new IllegalStateException(format("%s %s returned %s: %s", method, path, response.statusCode(),
					response.body()));
		}
		return response.body().isBlank() ? Map.of() : JSON.fromJson(response.body());
	}

	private HttpResponse<String> send(String method, String path, Object body, boolean authenticate) {
		HttpRequest.Builder request = HttpRequest.newBuilder(
				URI.create("https://" + container.getHost() + ":" + container.getMappedPort(API_PORT) + path))
				.timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json")
				.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
						: HttpRequest.BodyPublishers.ofString(JSON_BODY.toJson(body)));
		if (authenticate) {
			request.header("Authorization",
					"Basic " + Base64.getEncoder().encodeToString((USERNAME + ":" + password).getBytes(UTF_8)));
		}
		try {
			return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	private static void await(String description, BooleanSupplier condition) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			try {
				if (condition.getAsBoolean()) {
					return;
				}
			} catch (UncheckedIOException e) {
				// The REST API restarts while the cluster bootstraps
			}
			if (System.nanoTime() > deadline) {
				throw new IllegalStateException("Timed out waiting for " + description);
			}
			sleep(Duration.ofMillis(250));
		}
	}

	private static void sleep(Duration duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	// Docker's port proxy accepts connections before the database listens, so check that Redis answers
	private static boolean isPingable(String host, int port) {
		try (Socket socket = new Socket(host, port)) {
			socket.setSoTimeout(2000);
			socket.getOutputStream().write("PING\r\n".getBytes(UTF_8));
			byte[] reply = new byte[5];
			return socket.getInputStream().readNBytes(reply, 0, reply.length) == reply.length
					&& new String(reply, UTF_8).equals("+PONG");
		} catch (IOException e) {
			return false;
		}
	}

	// Ports that are free on this host now, in the database port range
	private static List<Integer> freeHostPorts() {
		List<Integer> ports = new ArrayList<>();
		while (ports.size() < DATABASE_PORTS) {
			int port = ThreadLocalRandom.current().nextInt(MIN_DATABASE_PORT, MAX_DATABASE_PORT + 1);
			if (ports.contains(port)) {
				continue;
			}
			try (ServerSocket socket = new ServerSocket(port)) {
				ports.add(port);
			} catch (IOException e) {
				// in use
			}
		}
		return ports;
	}

	// The node advertises this address to cluster clients in the test JVM
	private String hostAddress() {
		try {
			return InetAddress.getByName(container.getHost()).getHostAddress();
		} catch (UnknownHostException e) {
			throw new UncheckedIOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return (Map<String, Object>) value;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> list(Object value) {
		return (List<Object>) value;
	}

	// The node's REST API uses a self-signed certificate for its own host name
	private static SSLContext trustAll() {
		TrustManager trustAll = new X509ExtendedTrustManager() {
			@Override
			public void checkClientTrusted(X509Certificate[] chain, String authType) {
			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain, String authType) {
			}

			@Override
			public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
			}

			@Override
			public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return new X509Certificate[0];
			}
		};
		try {
			SSLContext context = SSLContext.getInstance("TLS");
			context.init(null, new TrustManager[] { trustAll }, null);
			return context;
		} catch (GeneralSecurityException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void close() {
		container.close();
	}
}
