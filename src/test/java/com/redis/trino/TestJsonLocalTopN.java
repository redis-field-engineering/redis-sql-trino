package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonFactory;

import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.BigintType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

public class TestJsonLocalTopN extends AbstractTestQueryFramework {
    private static final int ROWS = 137;
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }
    protected boolean resp2() { return false; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        QueryRunner runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.cursor-count", "7", "redisearch.query-timeout-ms", "30000", "redisearch.resp2", Boolean.toString(resp2())));
        Map<String, String> properties = new HashMap<>(server.getConnectorProperties());
        properties.putAll(Map.of("redisearch.default-schema-name", "tpch", "redisearch.cursor-count", "7",
                "redisearch.query-timeout-ms", "30000", "redisearch.resp2", Boolean.toString(resp2()), "redisearch.local-topn.max-candidates", "0"));
        runner.createCatalog("without_json_topn", "redisearch", properties);
        var redis = server.getConnection().sync();
        redis.ftCreate("json_topn", CreateArgs.builder().on(CreateArgs.TargetType.JSON).withPrefix("json_topn:").build(), List.of(
                NumericFieldArgs.builder().name("$.id").as("id").build(),
                NumericFieldArgs.builder().name("$.nested.rank").as("rank").build(),
                TextFieldArgs.builder().name("$.nested.label").as("label").build(),
                TagFieldArgs.builder().name("$.payload").as("payload").build(),
                TagFieldArgs.builder().name("$.flag").as("flag").build(),
                TagFieldArgs.builder().name("$.tags").as("tags").build(),
                TagFieldArgs.builder().name("$.missing").as("missing").build()));
        RediSearchColumnTypes.write(redis, "json_topn", Map.of("id", BigintType.BIGINT));
        List<String> keys = server.keysOnShards("json_topn:", IntStream.range(0, ROWS).map(i -> i % 2).toArray());
        double rank = 0.1234567890123456;
        for (int i = 0; i < ROWS; i++) {
            rank = Math.nextUp(rank);
            StringWriter text = new StringWriter();
            try (var out = new JsonFactory().createGenerator(text)) {
                out.writeStartObject();
                out.writeNumberField("id", 9007199254740992L + i);
                out.writeObjectFieldStart("nested");
                if (i % 17 != 0) {
                    if (i % 11 == 0) { out.writeNullField("rank"); }
                    else { out.writeNumberField("rank", rank); }
                }
                if (i % 19 != 0) {
                    if (i % 13 == 0) { out.writeNullField("label"); }
                    else { out.writeStringField("label", switch (i % 5) {
                        case 0 -> "needle suffix";
                        case 1 -> "needle";
                        case 2 -> "\uE000";
                        case 3 -> "𐀀";
                        default -> "quote\"\\\n";
                    }); }
                }
                out.writeEndObject();
                out.writeStringField("payload", "p".repeat(2048) + "-" + i);
                if (i % 9 != 0) {
                    if (i % 7 == 0) { out.writeNullField("flag"); }
                    else { out.writeBooleanField("flag", i % 2 == 0); }
                }
                out.writeArrayFieldStart("tags");
                out.writeString("a");
                out.writeString("b,c");
                out.writeEndArray();
                out.writeEndObject();
            }
            redis.jsonSet(keys.get(i), JsonPath.ROOT_PATH, text.toString());
        }
        server.awaitIndexed("json_topn");
        return runner;
    }

    @Test
    public void testExactNumericNestedAliasesAndLateHydrationMetrics() {
        String sql = "SELECT id, rank, payload FROM json_topn ORDER BY rank DESC NULLS FIRST, id ASC LIMIT 5";
        assertExactOrder(sql);
        Metrics metrics = metrics(sql);
        assertThat(metric(metrics, "redis.rows.received")).isEqualTo(ROWS);
        assertThat(metric(metrics, "redis.local-topn.candidates")).isEqualTo(5);
        assertThat(metric(metrics, "redis.exact-hash-commands")).isZero();
        assertThat(metric(metrics, "redis.json.commands")).isEqualTo(5);
        assertThat(metric(metrics, "redis.json.path-reads")).isEqualTo(15);
        assertThat(metric(metrics, "redis.local-topn.projected-json-reads")).isEqualTo(15);
        assertThat(metric(metrics, "redis.json.read-batches")).isEqualTo(1);
        System.out.println("JSON_TOPN_LATE_MATERIALIZATION deployment=" + deployment() + " resp2=" + resp2() + " metrics=" + metrics.getMetrics());
    }

    @Test
    public void testUnicodeNullBooleansArraysMissingAndRootProjection() {
        for (String sql : List.of(
                "SELECT id, label, payload FROM json_topn ORDER BY label ASC NULLS FIRST, rank DESC, id ASC LIMIT 5",
                "SELECT flag, tags, missing, \"$\" FROM json_topn ORDER BY rank ASC NULLS FIRST, id DESC LIMIT 4",
                "SELECT payload FROM json_topn ORDER BY __key DESC LIMIT 3",
                "SELECT \"$\" FROM json_topn ORDER BY __key ASC LIMIT 2")) {
            assertExactOrder(sql);
            assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isPositive();
        }
    }

    @Test
    public void testConnectorExactStringFilteringPrecedesSelection() {
        String sql = "SELECT id, rank, payload FROM json_topn WHERE label = 'needle' ORDER BY rank DESC NULLS LAST, id ASC LIMIT 5";
        assertExactOrder(sql);
        assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isEqualTo(5);
        String residual = "SELECT id, payload FROM json_topn WHERE flag = '1' ORDER BY rank DESC LIMIT 3";
        assertExactOrder(residual);
        assertThat(metric(metrics(residual), "redis.local-topn.candidates")).isZero();
    }

    @Test
    public void testUnsupportedWildcardSchemaRetainsFallback() {
        var redis = server.getConnection().sync();
        redis.ftCreate("json_topn_wild", CreateArgs.builder().on(CreateArgs.TargetType.JSON).withPrefix("json_topn_wild:").skipInitialScan().build(), List.of(
                NumericFieldArgs.builder().name("$.rank").as("rank").build(),
                TagFieldArgs.builder().name("$.tags[*]").as("tags").build(),
                TagFieldArgs.builder().name("$.payload").as("payload").build()));
        try {
            for (int i = 0; i < 8; i++) {
                redis.jsonSet("json_topn_wild:" + i, JsonPath.ROOT_PATH,
                        "{\"rank\":" + i + ",\"tags\":[\"a\",\"b\"],\"payload\":\"row" + i + "\"}");
            }
            String sql = "SELECT payload FROM json_topn_wild ORDER BY rank DESC LIMIT 2";
            assertExactOrder(sql);
            assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isZero();
            assertThat(metric(metrics(sql), "redis.json.commands")).isZero();
        } finally { assertUpdate("DROP TABLE json_topn_wild"); }
    }

    @Test
    public void testJsonHydrationMemoryFailureReleasesReservation() {
        var types = getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager();
        var session = new RediSearchSession(types, directConfig());
        AtomicLong reserved = new AtomicLong();
        try (var reader = session.exactHashReader()) {
            var schema = session.getTable(new SchemaTableName("tpch", "json_topn"));
            var output = schema.getColumns().stream().filter(column -> column.getName().equals("$")).findFirst().orElseThrow();
            var table = schema.getTableHandle().withTopN(List.of(new RediSearchSortItem("rank", true, true, true)), 5);
            assertThatThrownBy(() -> RediSearchLocalTopN.read(session, session.scanConnection(), table, List.of(output), reader,
                    new RediSearchReadStats(), bytes -> {
                        reserved.set(bytes);
                        if (bytes > 8000) { throw new IllegalStateException("json topn memory limit"); }
                    })).hasRootCauseMessage("json topn memory limit");
            assertThat(reserved.get()).isZero();
        } finally { session.shutdown(); }
    }

    @Test
    public void testCancellationDeletesJsonCursorWithoutHydration() {
        var types = getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager();
        AtomicReference<CompletableFuture<String>> deletion = new AtomicReference<>();
        var session = new RediSearchSession(types, directConfig()) {
            @Override
            public CompletableFuture<AggregationReply<String>> cursorReadAsync(Connection scan, RediSearchTableHandle table,
                    Cursor cursor, RediSearchReadStats stats, long count) {
                var future = super.cursorReadAsync(scan, table, cursor, stats, count);
                Thread.currentThread().interrupt();
                return future;
            }
            @Override
            public CompletableFuture<String> cursorDeleteAsync(Connection scan, RediSearchTableHandle table, Cursor cursor) {
                var future = super.cursorDeleteAsync(scan, table, cursor);
                deletion.set(future);
                return future;
            }
        };
        RediSearchReadStats stats = new RediSearchReadStats();
        AtomicLong reserved = new AtomicLong();
        try (var reader = session.exactHashReader()) {
            var schema = session.getTable(new SchemaTableName("tpch", "json_topn"));
            var output = schema.getColumns().stream().filter(column -> column.getName().equals("payload")).findFirst().orElseThrow();
            var table = schema.getTableHandle().withTopN(List.of(new RediSearchSortItem("rank", true, true)), 3);
            try {
                assertThatThrownBy(() -> RediSearchLocalTopN.read(session, session.scanConnection(), table, List.of(output), reader, stats, reserved::set))
                        .isInstanceOf(java.util.concurrent.CancellationException.class);
            } finally { Thread.interrupted(); }
            assertThat(stats.jsonCommands.sum()).isZero();
            assertThat(reserved.get()).isZero();
            assertThat(deletion.get()).isNotNull();
            assertThat(deletion.get().join()).isEqualTo("OK");
        } finally { Thread.interrupted(); session.shutdown(); }
    }

    @Test
    public void testLaterWarningReturnsNoJsonProjectionAndDeletesCursor() {
        var types = getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager();
        AtomicReference<CompletableFuture<String>> deletion = new AtomicReference<>();
        var session = new RediSearchSession(types, directConfig()) {
            @Override
            public CompletableFuture<AggregationReply<String>> cursorReadAsync(Connection scan, RediSearchTableHandle table,
                    Cursor cursor, RediSearchReadStats stats, long count) {
                return super.cursorReadAsync(scan, table, cursor, stats, count).thenApply(reply -> {
                    reply.getReplies().getFirst().getWarnings().add("Timeout limit was reached");
                    return reply;
                });
            }
            @Override
            public CompletableFuture<String> cursorDeleteAsync(Connection scan, RediSearchTableHandle table, Cursor cursor) {
                CompletableFuture<String> future = super.cursorDeleteAsync(scan, table, cursor);
                deletion.set(future);
                return future;
            }
        };
        RediSearchReadStats stats = new RediSearchReadStats();
        AtomicLong reserved = new AtomicLong();
        try (var reader = session.exactHashReader()) {
            var schema = session.getTable(new SchemaTableName("tpch", "json_topn"));
            var output = schema.getColumns().stream().filter(column -> column.getName().equals("payload")).findFirst().orElseThrow();
            var table = schema.getTableHandle().withTopN(List.of(new RediSearchSortItem("rank", true, true)), 3);
            assertThatThrownBy(() -> RediSearchLocalTopN.read(session, session.scanConnection(), table, List.of(output), reader, stats, reserved::set))
                    .hasMessageContaining("refusing a possibly incomplete result");
            assertThat(stats.jsonCommands.sum()).isZero();
            assertThat(reserved.get()).isZero();
            assertThat(deletion.get()).isNotNull();
            assertThat(deletion.get().join()).isEqualTo("OK");
        } finally { session.shutdown(); }
    }

    private RediSearchConfig directConfig() {
        return new RediSearchConfig().setUri(server.getRedisURI()).setCluster(deployment().isCluster())
                .setResp2(resp2()).setCursorCount(3).setQueryTimeoutMillis(30000);
    }

    private void assertExactOrder(String sql) {
        var actual = computeActual(sql);
        var expected = computeActual(sql.replace("FROM json_topn", "FROM without_json_topn.tpch.json_topn"));
        assertThat(actual.getTypes()).isEqualTo(expected.getTypes());
        assertThat(actual.getMaterializedRows().stream().map(io.trino.testing.MaterializedRow::getFields).toList())
                .containsExactlyElementsOf(expected.getMaterializedRows().stream().map(io.trino.testing.MaterializedRow::getFields).toList());
    }

    private Metrics metrics(String sql) {
        var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
    }

    private static long metric(Metrics metrics, String name) { return ((LongCount) metrics.getMetrics().get(name)).getTotal(); }
}
