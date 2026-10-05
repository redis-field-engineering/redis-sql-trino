package com.redis.trino;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;

/**
 * Times representative queries against TPC-H tiny data loaded into RediSearch.
 *
 * Not part of the default test run (surefire only picks up Test*, *Test and Benchmark* classes). Run it with:
 *
 * <pre>
 * ./mvnw test -Dtest=RediSearchQueryBenchmark -Dair.check.skip-all=true -Dbenchmark.label=head
 * </pre>
 *
 * Results are written to target/benchmark/&lt;label&gt;.csv; compare two runs with
 * .github/scripts/compare-benchmarks.py.
 */
public class RediSearchQueryBenchmark {

	private static final Logger log = Logger.get(RediSearchQueryBenchmark.class);

	private static final int WARMUP = Integer.getInteger("benchmark.warmup", 5);
	private static final int ITERATIONS = Integer.getInteger("benchmark.iterations", 20);
	private static final String LABEL = System.getProperty("benchmark.label", "results");
	private static final int BACKGROUND_SCANS = 2;

	private static final Map<String, String> QUERIES = ImmutableMap.<String, String>builder()
			.put("full_scan", "SELECT sum(quantity * extendedprice) FROM lineitem")
			.put("filter_numeric", "SELECT count(*), sum(extendedprice) FROM lineitem WHERE quantity < 10")
			.put("filter_tag", "SELECT count(*) FROM lineitem WHERE shipmode = 'AIR'")
			.put("group_by",
					"SELECT returnflag, linestatus, count(*), sum(quantity), avg(extendedprice) FROM lineitem GROUP BY returnflag, linestatus")
			.put("limit", "SELECT * FROM lineitem LIMIT 1000")
			.put("point_lookup", "SELECT * FROM orders WHERE orderkey = 7")
			.put("top_n", "SELECT orderkey, totalprice FROM orders ORDER BY totalprice DESC LIMIT 10")
			.put("join",
					"SELECT c.mktsegment, count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey GROUP BY c.mktsegment")
			.put("join_selective",
					"SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey WHERE c.nationkey = 3")
			.put("information_schema_columns",
					"SELECT count(*) FROM information_schema.columns WHERE table_schema = 'tpch'")
			.put("describe", "DESCRIBE lineitem")
			.buildOrThrow();

	@Test
	public void benchmark() throws Exception {
		try (RediSearchServer server = new RediSearchServer()) {
			server.getConnection().sync().flushall();
			try (DistributedQueryRunner queryRunner = RediSearchQueryRunner.createRediSearchQueryRunner(server,
					CUSTOMER, LINE_ITEM, NATION, ORDERS, REGION)) {
				// Trino's default, which the test query runner replaces with partitioning every join
				Session session = Session.builder(queryRunner.getDefaultSession())
						.setSystemProperty("join_distribution_type", "AUTOMATIC").build();
				Map<String, Result> results = new LinkedHashMap<>();
				for (Map.Entry<String, String> query : QUERIES.entrySet()) {
					results.put(query.getKey(), run(queryRunner, session, query.getValue(), Optional.empty()));
				}
				// Writes 15,000 hashes, which DROP TABLE deletes before the next run
				results.put("insert", run(queryRunner, session,
						"CREATE TABLE bench_orders AS SELECT * FROM tpch.tiny.orders",
						Optional.of("DROP TABLE bench_orders")));
				results.put("point_lookup_during_scans", runDuringScans(queryRunner, session, QUERIES.get("point_lookup"),
						QUERIES.get("full_scan")));
				write(results);
			}
		}
	}

	/**
	 * @param cleanup run after each run of the query, untimed
	 */
	private static Result run(DistributedQueryRunner queryRunner, Session session, String sql,
			Optional<String> cleanup) {
		long rows = 0;
		for (int i = 0; i < WARMUP; i++) {
			rows = queryRunner.execute(session, sql).getRowCount();
			cleanup.ifPresent(statement -> queryRunner.execute(session, statement));
		}
		double[] millis = new double[ITERATIONS];
		for (int i = 0; i < ITERATIONS; i++) {
			long start = System.nanoTime();
			MaterializedResult result = queryRunner.execute(session, sql);
			millis[i] = (System.nanoTime() - start) / 1_000_000.0;
			assertThat(result.getRowCount()).as("row count of %s", sql).isEqualTo(rows);
			cleanup.ifPresent(statement -> queryRunner.execute(session, statement));
		}
		return new Result(rows, millis);
	}

	// Times a query while BACKGROUND_SCANS threads run another one over and over
	private static Result runDuringScans(DistributedQueryRunner queryRunner, Session session, String sql,
			String background) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(BACKGROUND_SCANS);
		AtomicBoolean done = new AtomicBoolean();
		List<Future<?>> scans = new ArrayList<>();
		for (int i = 0; i < BACKGROUND_SCANS; i++) {
			scans.add(executor.submit(() -> {
				while (!done.get()) {
					queryRunner.execute(session, background);
				}
			}));
		}
		try {
			return run(queryRunner, session, sql, Optional.empty());
		} finally {
			done.set(true);
			for (Future<?> scan : scans) {
				scan.get();
			}
			executor.shutdown();
		}
	}

	private static void write(Map<String, Result> results) throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("query,rows,iterations,min_ms,median_ms,p90_ms,max_ms,samples_ms");
		StringBuilder summary = new StringBuilder(
				format("%n%-28s %8s %10s %10s %10s%n", "query", "rows", "median ms", "p90 ms", "min ms"));
		for (Map.Entry<String, Result> entry : results.entrySet()) {
			Result result = entry.getValue();
			lines.add(format(ENGLISH, "%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%s", entry.getKey(), result.rows(),
					result.sorted().length, result.percentile(0), result.percentile(50), result.percentile(90),
					result.percentile(100), result.samples()));
			summary.append(format(ENGLISH, "%-28s %8d %10.1f %10.1f %10.1f%n", entry.getKey(), result.rows(),
					result.percentile(50), result.percentile(90), result.percentile(0)));
		}
		Path file = Path.of("target", "benchmark", LABEL + ".csv");
		Files.createDirectories(file.getParent());
		Files.write(file, lines, UTF_8);
		log.info("Benchmark results written to %s:%s", file.toAbsolutePath(), summary);
	}

	private record Result(long rows, double[] millis) {

		double[] sorted() {
			double[] sorted = millis.clone();
			Arrays.sort(sorted);
			return sorted;
		}

		// Nearest-rank percentile
		double percentile(int percentile) {
			double[] sorted = sorted();
			int rank = (int) Math.ceil(percentile / 100.0 * sorted.length);
			return sorted[Math.max(rank, 1) - 1];
		}

		String samples() {
			return String.join(";", Arrays.stream(millis).mapToObj(value -> format(ENGLISH, "%.3f", value)).toList());
		}
	}
}
