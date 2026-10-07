package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.DurationTiming;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;

/**
 * Scans read the batches of a cursor ahead, and delete the cursor when Trino stops before the last one.
 */
public class TestCursorReads extends AbstractTestQueryFramework {

	private static final ProtocolKeyword FT_INFO = new ProtocolKeyword() {
		private final byte[] bytes = "FT.INFO".getBytes(StandardCharsets.US_ASCII);

		@Override
		public byte[] getBytes() {
			return bytes;
		}
	};

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private RediSearchServer redisearch;

	protected Deployment deployment() {
		return Deployment.NON_SHARDED;
	}

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = closeAfterClass(new RediSearchServer(deployment()));
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("many", CreateArgs.builder().withPrefix("many:").build(),
				List.of(NumericFieldArgs.builder().name("id").build()));
		redisearch.awaitIndexed("many");
		redisearch.writeHashes("many:", 100);
		// Batches of 7 rows
		return RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, List.of(), Map.of(),
				Map.of("redisearch.cursor-count", "7"));
	}

	@Test
	public void testReadsEveryBatch() {
		// Trino computes count(DISTINCT ...), and so all of them, over the 15 batches
		assertThat(query("SELECT count(id), count(DISTINCT id), min(id), max(id) FROM many"))
				.matches("VALUES (BIGINT '100', BIGINT '100', DOUBLE '1', DOUBLE '100')");
	}

	@Test
	public void testScanAndAggregationMetrics() {
		Metrics scan = connectorMetrics("SELECT count(DISTINCT id) FROM many");
		assertThat(count(scan, "redis.aggregate.requests")).isEqualTo(1);
		assertThat(count(scan, "redis.cursor.requests")).isPositive();
		assertThat(count(scan, "redis.rows.received")).isEqualTo(100);
		assertThat(count(scan, "redis.exact-hash-reads")).isZero();
		assertThat(((DurationTiming) scan.getMetrics().get("redis.request-wall-time")).getDuration())
				.isGreaterThan(Duration.ZERO);
		assertThat(((DurationTiming) scan.getMetrics().get("redis.row-conversion-time")).getDuration())
				.isGreaterThan(Duration.ZERO);
		Metrics aggregate = connectorMetrics("SELECT count(*) FROM many");
		assertThat(count(aggregate, "redis.rows.received")).isEqualTo(1);
		assertThat(count(aggregate, "redis.aggregate.requests")).isEqualTo(1);
	}

	@Test
	public void testExactHashReadMetrics() {
		assertUpdate("CREATE TABLE metric_bigints (id bigint, marker varchar)");
		try {
			assertUpdate("INSERT INTO metric_bigints VALUES (9007199254740993, 'large'), (7, 'small'), (NULL, 'missing')", 3);
			assertThat(query("SELECT id FROM metric_bigints"))
					.matches("VALUES BIGINT '9007199254740993', BIGINT '7', CAST(NULL AS BIGINT)");
			Metrics metrics = connectorMetrics("SELECT id FROM metric_bigints");
			assertThat(count(metrics, "redis.rows.received")).isEqualTo(3);
			assertThat(count(metrics, "redis.exact-hash-reads")).isEqualTo(1);
		} finally {
			assertUpdate("DROP TABLE metric_bigints");
		}
	}

	@Test
	public void testExactDistinctAcrossBatchesAndShards() {
		assertUpdate("CREATE TABLE exact_distinct (id bigint, other bigint, marker varchar)");
		try {
			// Adjacent values sharing an indexed double, both signs, extrema, zero and missing values. Each value
			// is repeated across cursor batches and (with the cluster deployment) both shards.
			List<String> values = java.util.Arrays.asList("9007199254740992", "9007199254740993", "9007199254740994",
					"-9007199254740992", "-9007199254740993", "-9223372036854775808", "9223372036854775807", "0", null);
			List<String> keys = redisearch.keysOnShards("exact_distinct:", IntStream.range(0, 227).map(i -> i % 2).toArray());
			for (int i = 0; i < keys.size(); i++) {
				Map<String, String> fields = new java.util.HashMap<>(Map.of("marker", "row", "other", "9223372036854775807"));
				String value = i < 225 ? values.get(i % values.size())
						: (i == 225 ? "9007199254740993.0" : "9.007199254740993e15");
				if (value != null) {
					fields.put("id", value);
				}
				redisearch.getConnection().sync().hset(keys.get(i), fields);
			}
			assertThat(query("SELECT count(DISTINCT id), count(id), min(id), max(id), count(DISTINCT other) FROM exact_distinct"))
					.matches("VALUES (BIGINT '8', BIGINT '202', BIGINT '-9223372036854775808', BIGINT '9223372036854775807', BIGINT '1')");
			Metrics metrics = connectorMetrics("SELECT id, other FROM exact_distinct");
			assertThat(count(metrics, "redis.rows.received")).isEqualTo(227);
			assertThat(count(metrics, "redis.exact-hash-reads")).isEqualTo(404);
			assertThat(count(metrics, "redis.exact-hash-read-batches")).isPositive()
					.isLessThan(count(metrics, "redis.exact-hash-reads"));
			assertThat(query("SELECT count(*) FROM (SELECT id FROM exact_distinct WHERE id % 2 = 0 LIMIT 3)"))
					.matches("VALUES BIGINT '3'");
			// A limited scan closes its private reader without changing flushing on connections used by other scans.
			assertThat(query("SELECT count(DISTINCT id) FROM many")).matches("VALUES BIGINT '100'");
		} finally {
			assertUpdate("DROP TABLE exact_distinct");
		}
	}

	private Metrics connectorMetrics(String sql) {
		QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
		return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
				.getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
				.reduce(Metrics.EMPTY, Metrics::mergeWith);
	}

	private static long count(Metrics metrics, String name) {
		return ((LongCount) metrics.getMetrics().get(name)).getTotal();
	}

	@Test
	public void testDeletesCursorWhenTrinoStops() throws InterruptedException {
		// Trino filters, and stops once it has 3 rows, while the next batches are read
		assertThat(query("SELECT count(*) FROM (SELECT id FROM many WHERE id % 2 = 0 LIMIT 3)"))
				.matches("VALUES BIGINT '3'");
		if (deployment().isCluster()) {
			// FT.INFO reports the cursors of the shard it's sent to
			return;
		}
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (openCursors() > 0) {
			assertThat(System.nanoTime()).as("cursors deleted within %s", TIMEOUT).isLessThan(deadline);
			Thread.sleep(100);
		}
	}

	// The index's cursors, which other tests' queries may also have open
	private long openCursors() {
		List<Object> info = redisearch.getConnection().sync().dispatch(FT_INFO,
				new NestedMultiOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add("many"));
		List<?> stats = (List<?>) info.get(info.indexOf("cursor_stats") + 1);
		return (Long) stats.get(stats.indexOf("index_total") + 1);
	}
}
