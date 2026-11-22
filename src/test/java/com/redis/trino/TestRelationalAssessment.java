package com.redis.trino;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import io.airlift.json.JsonCodec;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;

/** Bounded relational correctness/plan assessment. Timed queries run against one immutable fixture. */
public class TestRelationalAssessment extends AbstractTestQueryFramework {
    private static final JsonCodec<Object> JSON = JsonCodec.jsonCodec(Object.class);
    private static final int REPEATS = 4;
    private RediSearchServer server;
    private static final List<Fact> SEED = List.of(
            new Fact("a", "A", "X", 10), new Fact("a", "A", "X", 10),
            new Fact("b", "A", "Y", null), new Fact("b", null, "Y", 20),
            new Fact(null, null, null, 30), new Fact("c", "B", "X", null),
            new Fact("d", "B", null, 5), new Fact("d", "B", null, 5));
    private static final List<Dimension> DIMENSIONS = List.of(new Dimension("a", "first"), new Dimension("a", "second"),
            new Dimension("b", "only"), new Dimension(null, "null-key"), new Dimension("z", "unmatched"), new Dimension("c", null));

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        redis.ftCreate("rel_facts", CreateArgs.builder().withPrefix("rel_fact:").skipInitialScan().build(), List.of(
                NumericFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("k").build(),
                TagFieldArgs.builder().name("g").build(), TagFieldArgs.builder().name("h").build(), NumericFieldArgs.builder().name("v").build()));
        redis.ftCreate("rel_dims", CreateArgs.builder().withPrefix("rel_dim:").skipInitialScan().build(), List.of(
                TagFieldArgs.builder().name("k").build(), TagFieldArgs.builder().name("label").build()));
        RediSearchColumnTypes.write(redis, "rel_facts", Map.of("id", io.trino.spi.type.IntegerType.INTEGER, "v", io.trino.spi.type.IntegerType.INTEGER));
        List<String> factKeys = server.keysOnShards("rel_fact:", java.util.stream.IntStream.range(0, SEED.size() * REPEATS).map(i -> i % 2).toArray());
        for (int i = 0; i < factKeys.size(); i++) {
            Fact row = SEED.get(i % SEED.size());
            Map<String, String> fields = new HashMap<>(Map.of("id", Integer.toString(i)));
            put(fields, "k", row.k()); put(fields, "g", row.g()); put(fields, "h", row.h());
            if (row.v() != null) { fields.put("v", row.v().toString()); }
            redis.hset(factKeys.get(i), fields);
        }
        List<String> dimensionKeys = server.keysOnShards("rel_dim:", 0, 1, 0, 1, 0, 1);
        for (int i = 0; i < DIMENSIONS.size(); i++) {
            Dimension row = DIMENSIONS.get(i);
            Map<String, String> fields = new HashMap<>();
            put(fields, "k", row.k()); put(fields, "label", row.label());
            redis.hset(dimensionKeys.get(i), fields);
        }
        server.awaitIndexed("rel_facts"); server.awaitIndexed("rel_dims");
        QueryRunner runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(), Map.of("redisearch.cursor-count", "5"));
        Map<String, String> properties = new HashMap<>(server.getConnectorProperties());
        properties.put("redisearch.default-schema-name", "tpch"); properties.put("redisearch.cursor-count", "5");
        runner.createCatalog("relational_peer", "redisearch", properties);
        return runner;
    }

    @Test
    public void testBoundedRelationalAssessment() throws Exception {
        int attempts = Integer.getInteger("redisearch.relational-assessment.repeats", 1);
        assertThat(attempts).isBetween(1, 5);
        Session enabled = Session.builder(getSession()).setSystemProperty("enable_dynamic_filtering", "false").build();
        Session disabled = Session.builder(enabled).setSystemProperty("allow_pushdown_into_connectors", "false").build();
        String output = System.getProperty("redisearch.relational-assessment.output");
        Path directory = output == null ? null : Path.of(output).resolve(deployment().name().toLowerCase(java.util.Locale.ENGLISH));
        if (directory != null) { Files.createDirectories(directory); }
        List<Map<String, Object>> samples = new ArrayList<>();
        for (Workload workload : workloads()) {
            String oracleSql = valuesCte() + workload.oracleSql().replace("relational_peer.tpch.rel_dims", "rel_dims");
            MaterializedResult oracle = getQueryRunner().execute(disabled, oracleSql);
            if (directory != null) { Files.writeString(directory.resolve(workload.name() + "-oracle.sql"), oracleSql, UTF_8); }
            for (int attempt = 1; attempt <= attempts; attempt++) {
                QueryRunner.MaterializedResultWithPlan pushed = execute(enabled, workload, oracle, "enabled", attempt, directory, samples);
                QueryRunner.MaterializedResultWithPlan fallback = execute(disabled, workload, oracle, "disabled", attempt, directory, samples);
                if (workload.name().equals("supported_aggregate_control")) {
                    assertThat(metric(metrics(pushed), "redis.rows.received")).isEqualTo(3);
                    assertThat(metric(metrics(fallback), "redis.rows.received")).isEqualTo(SEED.size() * REPEATS);
                }
                if (workload.name().equals("manual_weighted_grouping_sets") || workload.name().equals("manual_weighted_duplicate_sets")) {
                    assertThat(metric(metrics(pushed), "redis.rows.received")).isEqualTo(6);
                    assertThat(metric(metrics(fallback), "redis.rows.received")).isEqualTo(SEED.size() * REPEATS);
                }
                if (workload.name().equals("manual_preaggregate_join")) {
                    assertThat(metric(metrics(pushed), "redis.rows.received")).isEqualTo(5 + DIMENSIONS.size());
                    assertThat(metric(metrics(fallback), "redis.rows.received")).isEqualTo(SEED.size() * REPEATS + DIMENSIONS.size());
                }
            }
        }
        if (directory != null) {
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("deployment", deployment().name()); evidence.put("factRows", SEED.size() * REPEATS);
            evidence.put("dimensionRows", DIMENSIONS.size()); evidence.put("attemptsPerMode", attempts);
            evidence.put("fixtureSqlSha256", sha256(valuesCte())); evidence.put("fixtureSql", valuesCte());
            evidence.put("dynamicFiltering", false); evidence.put("immutableDuringMeasuredQueries", true);
            evidence.put("concurrencyContract", "No concurrent writes in this cohort. Connector reads are not snapshots; no multi-source atomicity is claimed.");
            evidence.put("timingScope", "In-process distributed test runner wall time including planning/execution/result fetch; sequential paired repetitions with uncontrolled cache state, not full-data throughput or causal speedup.");
            evidence.put("manualPreaggregation", "Explicit SQL rewrites only; not transparent connector join/grouping-set pushdown. Weighted grouping sets are COUNT-only, use eligible exact finest-grain keys, and retain outer Trino grouping identity.");
            evidence.put("crossCatalog", "Second Redis catalog over same immutable endpoint; plan boundary correctness only, not network-distribution performance.");
            evidence.put("javaVersion", System.getProperty("java.version")); evidence.put("samples", samples);
            Files.writeString(directory.resolve("assessment.json"), JSON.toJson(evidence), UTF_8);
        }
    }

    @Test
    public void testReadUncommittedVisibilityAndStrongerIsolationRejection() throws Exception {
        var redis = server.getConnection().sync();
        String index = "rel_visibility";
        redis.ftCreate(index, CreateArgs.builder().withPrefix(index + ":").skipInitialScan().build(),
                List.of(NumericFieldArgs.builder().name("v").build()));
        redis.hset(index + ":one", "v", "1");
        server.awaitIndexed(index);
        var manager = getQueryRunner().getTransactionManager();
        try {
            var transaction = manager.beginTransaction(io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED, true, false);
            Session session = Session.builder(getSession()).setTransactionId(transaction).setClientTransactionSupport().build();
            try {
                assertThat(getQueryRunner().execute(session, "SELECT v FROM " + index).getOnlyValue()).isEqualTo(1.0);
                // Deliberate, ordered external mutation on a separate probe; never modifies the timed fixture.
                redis.hset(index + ":one", "v", "2");
                assertThat(getQueryRunner().execute(session, "SELECT v FROM " + index).getOnlyValue()).isEqualTo(2.0);
            } finally { manager.asyncAbort(transaction).get(); }
            for (var isolation : List.of(io.trino.spi.transaction.IsolationLevel.READ_COMMITTED,
                    io.trino.spi.transaction.IsolationLevel.REPEATABLE_READ, io.trino.spi.transaction.IsolationLevel.SERIALIZABLE)) {
                var unsupported = manager.beginTransaction(isolation, true, false);
                Session stronger = Session.builder(getSession()).setTransactionId(unsupported).setClientTransactionSupport().build();
                try {
                    assertThatThrownBy(() -> getQueryRunner().execute(stronger, "SELECT v FROM " + index))
                            .hasStackTraceContaining("does not meet requested isolation level");
                } finally { manager.asyncAbort(unsupported).get(); }
            }
        } finally {
            redis.hset(index + ":one", "v", "1");
            redis.ftDropindex(index, true);
        }
    }

    private QueryRunner.MaterializedResultWithPlan execute(Session session, Workload workload, MaterializedResult oracle,
            String mode, int attempt, Path directory, List<Map<String, Object>> samples) throws Exception {
        long started = System.nanoTime();
        var result = getDistributedQueryRunner().executeWithPlan(session, workload.sql());
        long elapsed = System.nanoTime() - started;
        assertThat(result.result().getTypes()).as(workload.name() + " " + mode).isEqualTo(oracle.getTypes());
        // A multiset comparison preserves duplicate matches and duplicate grouping-set rows.
        assertThat(result.result().getMaterializedRows()).as(workload.name() + " " + mode)
                .containsExactlyInAnyOrderElementsOf(oracle.getMaterializedRows());
        List<String> operators = new ArrayList<>();
        collectOperators(result.queryPlan().orElseThrow().getRoot(), operators);
        assertThat(operators).as(workload.name()).containsAll(workload.requiredOperators());
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("workload", workload.name()); sample.put("mode", mode); sample.put("attempt", attempt);
        sample.put("elapsedNanos", elapsed); sample.put("rows", result.result().getRowCount()); sample.put("operators", operators);
        sample.put("types", result.result().getTypes().stream().map(Object::toString).toList());
        sample.put("resultSha256", resultHash(result.result())); sample.put("oracleSha256", resultHash(oracle));
        sample.put("validatedAgainstValues", true); sample.put("queryId", result.queryId().toString());
        Map<String, Long> counters = new TreeMap<>();
        metrics(result).getMetrics().forEach((name, value) -> { if (value instanceof LongCount count) { counters.put(name, count.getTotal()); } });
        sample.put("connectorMetrics", counters);
        sample.put("operatorStats", getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(operator -> Map.<String, Object>of(
                        "stageId", operator.getStageId(), "pipelineId", operator.getPipelineId(),
                        "planNodeId", operator.getPlanNodeId().toString(), "operatorType", operator.getOperatorType(),
                        "inputPositions", operator.getInputPositions(), "outputPositions", operator.getOutputPositions(),
                        "physicalInputPositions", operator.getPhysicalInputPositions(), "physicalInputBytes", operator.getPhysicalInputDataSize().toBytes(),
                        "cpuNanos", operator.getAddInputCpu().roundTo(java.util.concurrent.TimeUnit.NANOSECONDS)
                                + operator.getGetOutputCpu().roundTo(java.util.concurrent.TimeUnit.NANOSECONDS)
                                + operator.getFinishCpu().roundTo(java.util.concurrent.TimeUnit.NANOSECONDS))).toList());
        samples.add(sample);
        if (directory != null && attempt == 1) {
            String stem = workload.name() + "-" + mode + "-" + attempt;
            Files.writeString(directory.resolve(stem + "-rows.json"), JSON.toJson(result.result().getMaterializedRows().stream().map(MaterializedRow::getFields).toList()), UTF_8);
            // EXPLAIN is a separate untimed planning operation over the same immutable fixture.
            Files.writeString(directory.resolve(stem + "-plan.txt"), getQueryRunner().execute(session, "EXPLAIN " + workload.sql()).getOnlyValue().toString(), UTF_8);
            Files.writeString(directory.resolve(workload.name() + ".sql"), workload.sql(), UTF_8);
        }
        return result;
    }

    private static List<Workload> workloads() {
        return List.of(
                new Workload("inner_join", "SELECT f.id, f.k, f.v, d.label FROM rel_facts f JOIN rel_dims d ON f.k = d.k", Set.of("JoinNode")),
                new Workload("left_join", "SELECT f.id, f.k, d.label FROM rel_facts f LEFT JOIN rel_dims d ON f.k = d.k AND d.label <> 'second'", Set.of("JoinNode")),
                new Workload("full_join", "SELECT f.id, f.k, d.k, d.label FROM rel_facts f FULL JOIN rel_dims d ON f.k = d.k", Set.of("JoinNode")),
                new Workload("cross_catalog_join", "SELECT f.id, d.label FROM rel_facts f JOIN relational_peer.tpch.rel_dims d ON f.k = d.k", Set.of("JoinNode")),
                new Workload("row_number", "SELECT id, g, v, row_number() OVER (PARTITION BY g ORDER BY v NULLS LAST, id) AS rn FROM rel_facts", Set.of("WindowNode")),
                new Workload("window_sum_frames", "SELECT id, g, v, sum(v) OVER (PARTITION BY g ORDER BY v NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_sum, sum(v) OVER (PARTITION BY g ORDER BY v NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peer_sum FROM rel_facts", Set.of("WindowNode")),
                new Workload("rollup", "SELECT g, h, grouping(g,h) AS identity, count(*), sum(v) FROM rel_facts GROUP BY ROLLUP(g,h)", Set.of("GroupIdNode", "AggregationNode")),
                new Workload("cube", "SELECT g, h, grouping(g,h) AS identity, count(*), sum(v) FROM rel_facts GROUP BY CUBE(g,h)", Set.of("GroupIdNode", "AggregationNode")),
                new Workload("grouping_sets", "SELECT g, h, grouping(g,h) AS identity, count(*), sum(v) FROM rel_facts GROUP BY GROUPING SETS ((g,h),(g),(h),())", Set.of("GroupIdNode", "AggregationNode")),
                new Workload("duplicate_grouping_sets", "SELECT g, grouping(g) AS identity, count(*), sum(v) FROM rel_facts GROUP BY ALL GROUPING SETS ((g),(g),())", Set.of("GroupIdNode", "AggregationNode")),
                new Workload("mixed_aggregates", "SELECT g, count(*), min(v), array_agg(v ORDER BY id) FROM rel_facts GROUP BY g", Set.of("AggregationNode")),
                new Workload("manual_weighted_grouping_sets",
                        "SELECT g, h, grouping(g,h) AS identity, coalesce(sum(n), BIGINT '0') FROM (SELECT g, h, count(*) AS n FROM rel_facts GROUP BY g,h) p GROUP BY GROUPING SETS ((g,h),(g),())",
                        Set.of("GroupIdNode", "AggregationNode"),
                        "SELECT g, h, grouping(g,h) AS identity, count(*) FROM rel_facts GROUP BY GROUPING SETS ((g,h),(g),())"),
                new Workload("manual_weighted_duplicate_sets",
                        "SELECT g, h, grouping(g,h) AS identity, coalesce(sum(n), BIGINT '0') FROM (SELECT g, h, count(*) AS n FROM rel_facts GROUP BY g,h) p GROUP BY ALL GROUPING SETS ((g,h),(g),(g),())",
                        Set.of("GroupIdNode", "AggregationNode"),
                        "SELECT g, h, grouping(g,h) AS identity, count(*) FROM rel_facts GROUP BY ALL GROUPING SETS ((g,h),(g),(g),())"),
                new Workload("manual_weighted_empty_sets",
                        "SELECT g, h, grouping(g,h) AS identity, coalesce(sum(n), BIGINT '0') FROM (SELECT g, h, count(*) AS n FROM rel_facts WHERE id < 0 GROUP BY g,h) p GROUP BY ALL GROUPING SETS ((g,h),(g),(g),())",
                        Set.of(),
                        "SELECT g, h, grouping(g,h) AS identity, count(*) FROM rel_facts WHERE id < 0 GROUP BY ALL GROUPING SETS ((g,h),(g),(g),())"),
                new Workload("supported_aggregate_control", "SELECT g, count(*), min(v) FROM rel_facts GROUP BY g", Set.of()),
                new Workload("manual_preaggregate_join", "SELECT a.k, a.c, d.label FROM (SELECT k, count(*) AS c FROM rel_facts GROUP BY k) a LEFT JOIN rel_dims d ON a.k = d.k", Set.of("JoinNode")));
    }

    private static String valuesCte() {
        List<String> facts = new ArrayList<>();
        for (int i = 0; i < SEED.size() * REPEATS; i++) {
            Fact row = SEED.get(i % SEED.size());
            facts.add("(INTEGER '" + i + "'," + varchar(row.k()) + "," + varchar(row.g()) + "," + varchar(row.h()) + ","
                    + (row.v() == null ? "CAST(NULL AS integer)" : "INTEGER '" + row.v() + "'") + ")");
        }
        return "WITH rel_facts(id,k,g,h,v) AS (VALUES " + String.join(",", facts) + "), rel_dims(k,label) AS (VALUES "
                + String.join(",", DIMENSIONS.stream().map(row -> "(" + varchar(row.k()) + "," + varchar(row.label()) + ")").toList()) + ") ";
    }

    private Metrics metrics(QueryRunner.MaterializedResultWithPlan result) {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics).reduce(Metrics.EMPTY, Metrics::mergeWith);
    }
    private static long metric(Metrics metrics, String name) { return ((LongCount) metrics.getMetrics().get(name)).getTotal(); }
    private static void collectOperators(PlanNode node, List<String> operators) {
        operators.add(node.getClass().getSimpleName()); node.getSources().forEach(source -> collectOperators(source, operators));
    }
    private static String resultHash(MaterializedResult result) throws Exception {
        List<String> rows = result.getMaterializedRows().stream().map(row -> JSON.toJson(row.getFields())).sorted().toList();
        return sha256(JSON.toJson(List.of(result.getTypes().stream().map(Object::toString).toList(), rows)));
    }
    private static String sha256(String value) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(UTF_8))); }
    private static String varchar(String value) { return value == null ? "CAST(NULL AS varchar)" : "VARCHAR '" + value.replace("'", "''") + "'"; }
    private static void put(Map<String, String> fields, String name, String value) { if (value != null) { fields.put(name, value); } }
    private record Fact(String k, String g, String h, Integer v) {}
    private record Dimension(String k, String label) {}
    private record Workload(String name, String sql, Set<String> requiredOperators, String oracleSql) {
        Workload(String name, String sql, Set<String> requiredOperators) { this(name, sql, requiredOperators, sql); }
    }
}
