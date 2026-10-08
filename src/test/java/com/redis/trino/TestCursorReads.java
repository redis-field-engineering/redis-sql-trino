package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.AggregationReply.Cursor;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.spi.TrinoException;

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

	protected RediSearchServer redisearch;

    protected Deployment deployment() {
		return Deployment.NON_SHARDED;
    }

    @Test
    public void testRejectsInitialWarningAndDeletesItsCursor() throws InterruptedException {
        redisearch.getConnection().sync().ftCreate("warned_initial", CreateArgs.builder().withPrefix("warned_initial:").build(),
                List.of(NumericFieldArgs.builder().name("id").build()));
        redisearch.writeHashes("warned_initial:", 100);
        redisearch.awaitIndexed("warned_initial");
        TypeManager unusedTypes = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(), new Class<?>[] {TypeManager.class},
                (proxy, method, args) -> { throw new UnsupportedOperationException("No SQL type lookup in this scan"); });
        AtomicReference<Cursor> initialCursor = new AtomicReference<>();
        RediSearchSession session = new RediSearchSession(unusedTypes, new RediSearchConfig()
                .setUri(redisearch.getRedisURI()).setCluster(deployment().isCluster()).setCursorCount(7)) {
            @Override
            public AggregateResult result(ExactHashReader exactReader, RediSearchRowReader reader, Optional<Cursor> cursor,
                    AggregationReply<String> reply, RediSearchReadStats stats) {
                initialCursor.set(reply.getCursor().orElseThrow());
                // Inject a warning into a real live-cursor reply before the page source can take ownership.
                reply.getReplies().get(0).getWarnings().add("Timeout limit was reached");
                return super.result(exactReader, reader, cursor, reply, stats);
            }
        };
        try (var reader = session.exactHashReader()) {
            var table = new RediSearchTableHandle(new SchemaTableName("default", "warned_initial"), "warned_initial");
            var column = new RediSearchColumnHandle("id", io.trino.spi.type.DoubleType.DOUBLE,
                    RediSearchFieldType.NUMERIC, false, true, Optional.empty());
            assertThatThrownBy(() -> session.aggregate(session.scanConnection(), table, List.of(column), reader,
                    new RediSearchReadStats())).isInstanceOfSatisfying(TrinoException.class,
                            failure -> assertThat(failure.getErrorCode())
                                    .isEqualTo(RediSearchErrorCode.REDISEARCH_INCOMPLETE_RESULT.toErrorCode()));
            assertThat(initialCursor.get().getCursorId()).isPositive();
            // Use the actual owning node; the other shard's FT.INFO cannot prove this cursor was deleted.
            var connection = session.getConnection();
            StatefulRedisConnection<String, String> owner = connection instanceof StatefulRedisClusterConnection<String, String> cluster
                    ? cluster.getConnection(initialCursor.get().getNodeId().orElseThrow())
                    : (StatefulRedisConnection<String, String>) connection;
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (true) {
                List<Object> info = owner.sync().dispatch(FT_INFO,
                        new NestedMultiOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add("warned_initial"));
                List<?> stats = (List<?>) info.get(info.indexOf("cursor_stats") + 1);
                if ((Long) stats.get(stats.indexOf("index_total") + 1) == 0) { break; }
                assertThat(System.nanoTime()).as("initial cursor deleted within %s", TIMEOUT).isLessThan(deadline);
                Thread.sleep(10);
            }
            assertThatThrownBy(() -> owner.sync().ftCursorread("warned_initial", initialCursor.get(), 1))
                    .isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining("Cursor not found");
        } finally {
            session.shutdown();
        }
    }

	protected Map<String, String> scanProperties() {
		return Map.of("redisearch.cursor-count", "7");
	}

	protected long scanAggregateRequests() { return 1; }

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
				scanProperties());
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
		assertThat(count(scan, "redis.aggregate.requests")).isEqualTo(scanAggregateRequests());
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

	@Test
	public void testParamsFieldProjection() {
		// The class database owns this table and removes it at teardown, so a cleanup error cannot mask the
		// parser's original response if this projection makes the coordinator unhealthy.
		assertUpdate("CREATE TABLE reserved_params (params varchar, id bigint, marker varchar)");
		assertUpdate("INSERT INTO reserved_params VALUES ('value', 9007199254740993, 'row'), (NULL, 7, 'row')", 2);
		assertThat(query("SELECT params, id FROM reserved_params"))
				.matches("VALUES (VARCHAR 'value', BIGINT '9007199254740993'), (CAST(NULL AS VARCHAR), BIGINT '7')");
	}

	@Test
	public void testWideFilteredProjection() {
		// Q24's 105-field shape, including a DOUBLE that requires LOAD * and exact BIGINTs.
		String extras = IntStream.range(0, 99).mapToObj(i -> "c" + i + " integer")
				.collect(java.util.stream.Collectors.joining(", "));
		assertUpdate("CREATE TABLE wide_projection (url varchar, eventtime timestamp(3), userid bigint, score double, eventdate date, params varchar, "
				+ extras + ")");
		try {
			String extraValues = IntStream.range(0, 99).mapToObj(Integer::toString)
					.collect(java.util.stream.Collectors.joining(", "));
			String values = IntStream.range(0, 30).mapToObj(i -> "('" + (i % 2 == 0 ? "https://google/" : "other/")
					+ i + "', TIMESTAMP '2026-10-07 00:00:" + String.format("%02d", i)
					+ "', " + (9007199254740993L + i) + ", 0.1234567890123456, DATE '2026-10-07', VARCHAR 'value', " + extraValues + ")")
					.collect(java.util.stream.Collectors.joining(", "));
			assertUpdate("INSERT INTO wide_projection VALUES " + values, 30);
			String expected = IntStream.range(0, 10).mapToObj(n -> {
				int i = n * 2;
				return "(VARCHAR 'https://google/" + i + "', TIMESTAMP '2026-10-07 00:00:"
						+ String.format("%02d", i) + ".000', BIGINT '" + (9007199254740993L + i)
						+ "', DOUBLE '0.1234567890123456', DATE '2026-10-07', VARCHAR 'value', " + extraValues + ")";
			}).collect(java.util.stream.Collectors.joining(", "));
			assertThat(query("SELECT * FROM wide_projection WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10"))
					.matches("VALUES " + expected);
			// ClickBench has no DOUBLE columns: loading all its indexed fields by name must also be safe.
			String names = "url, eventtime, userid, eventdate, params, " + IntStream.range(0, 99)
					.mapToObj(i -> "c" + i).collect(java.util.stream.Collectors.joining(", "));
			String aliases = "url, eventtime, userid, score, eventdate, params, " + IntStream.range(0, 99)
					.mapToObj(i -> "c" + i).collect(java.util.stream.Collectors.joining(", "));
			assertThat(query("SELECT " + names + " FROM wide_projection WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10"))
					.matches("SELECT " + names + " FROM (VALUES " + expected + ") AS expected(" + aliases + ")");
		} finally {
			assertUpdate("DROP TABLE wide_projection");
		}
	}

	protected Metrics connectorMetrics(String sql) {
		QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
		return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
				.getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
				.reduce(Metrics.EMPTY, Metrics::mergeWith);
	}

	protected static long count(Metrics metrics, String name) {
		return ((LongCount) metrics.getMetrics().get(name)).getTotal();
	}

	@Test
	public void testDeletesCursorWhenTrinoStops() throws InterruptedException {
		// Trino filters, and stops once it has 3 rows, while the next batches are read
		assertThat(query("SELECT count(*) FROM (SELECT id FROM many WHERE id % 2 = 0 LIMIT 3)"))
				.matches("VALUES BIGINT '3'");
		if (deployment().isCluster()) {
			var client = io.lettuce.core.cluster.RedisClusterClient.create(redisearch.getRedisURI());
			try (var connection = client.connect()) {
				awaitCursorsDeleted(() -> java.util.stream.StreamSupport.stream(connection.getPartitions().spliterator(), false)
						.mapToLong(node -> openCursors(connection.getConnection(node.getNodeId()).sync())).sum());
			} finally {
				client.shutdown();
		}
		} else {
			awaitCursorsDeleted(() -> openCursors(redisearch.getConnection().sync()));
		}
	}

	private void awaitCursorsDeleted(java.util.function.LongSupplier count) throws InterruptedException {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (count.getAsLong() > 0) {
			assertThat(System.nanoTime()).as("cursors deleted within %s", TIMEOUT).isLessThan(deadline);
			Thread.sleep(100);
		}
	}

	// The index's cursors, which other tests' queries may also have open
	private long openCursors(RedisCommands<String, String> commands) {
		List<Object> info = commands.dispatch(FT_INFO,
				new NestedMultiOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8).add("many"));
		List<?> stats = (List<?>) info.get(info.indexOf("cursor_stats") + 1);
		return (Long) stats.get(stats.indexOf("index_total") + 1);
	}
}
