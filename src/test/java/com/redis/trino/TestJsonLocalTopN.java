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
    public void testModernJsonPathNativeValueParity() throws Exception {
        var redis = server.getConnection().sync();
        Map<String, String> paths = new java.util.LinkedHashMap<>();
        paths.put("id", "$.id");
        paths.put("space", "$['space key']");
        paths.put("dotted", "$[\"dot.key\"]");
        paths.put("unicode", "$['雪']");
        paths.put("quote", "$[\"quote\\\"key\"]");
        paths.put("slash", "$[\"slash\\\\key\"]");
        paths.put("members", "$['space key','dot.key']");
        paths.put("item", "$.items[1]");
        paths.put("last_item", "$.items[-1]");
        paths.put("wild", "$.tags[*]");
        paths.put("slice", "$.tags[0:2]");
        paths.put("stepped", "$.tags[0:3:2]");
        paths.put("negative_slice", "$.tags[-2:]");
        paths.put("all_slice", "$.tags[:]");
        paths.put("negative_union", "$.items[-1,0]");
        paths.put("union_value", "$.tags[0,2]");
        paths.put("recursive_value", "$..leaf");
        paths.put("object_wild", "$.nested.*");
        paths.put("object_leaf", "$.objects[*].leaf");
        paths.put("nums", "$.nums[*]");
        List<io.lettuce.core.search.arguments.FieldArgs> fields = new ArrayList<>();
        paths.forEach((alias, path) -> fields.add(alias.equals("id") || alias.endsWith("nums")
                ? NumericFieldArgs.builder().name(path).as(alias).build()
                : TagFieldArgs.builder().name(path).as(alias).build()));
        redis.ftCreate("json_topn_paths", CreateArgs.builder().on(CreateArgs.TargetType.JSON)
                .withPrefix("json_topn_paths:").skipInitialScan().build(), fields);
        try {
            for (int i = 0; i < 17; i++) {
                StringWriter text = new StringWriter();
                try (var out = new JsonFactory().createGenerator(text)) {
                    out.writeStartObject();
                    out.writeNumberField("id", 9007199254740992L + i);
                    if (i % 5 != 0) {
                        if (i % 3 == 0) { out.writeNullField("space key"); }
                        else { out.writeStringField("space key", "s" + i); }
                    }
                    out.writeStringField("dot.key", "p".repeat(1024) + i);
                    out.writeStringField("quote\"key", "quoted" + i);
                    out.writeStringField("slash\\key", "slashed" + i);
                    out.writeStringField("雪", i % 2 == 0 ? "𐀀" : "\uE000");
                    out.writeArrayFieldStart("items");
                    if (i % 4 != 0) { out.writeString("z" + i); out.writeString("b" + i); out.writeString("a" + i); }
                    out.writeEndArray();
                    out.writeArrayFieldStart("tags");
                    if (i % 4 != 0) {
                        if (i % 3 == 0) { out.writeNull(); } else { out.writeString("z" + i); }
                        out.writeString("c" + i); out.writeString("a" + i);
                    }
                    out.writeEndArray();
                    out.writeObjectFieldStart("nested"); out.writeStringField("leaf", "nested" + i);
                    out.writeStringField("other", "other" + i); out.writeEndObject();
                    out.writeArrayFieldStart("objects");
                    out.writeStartObject(); out.writeStringField("leaf", "first" + i); out.writeEndObject();
                    out.writeStartObject(); out.writeStringField("leaf", "second" + i); out.writeEndObject();
                    out.writeEndArray();
                    out.writeArrayFieldStart("nums");
                    if (i % 5 != 0) {
                        if (i % 3 == 0) { out.writeNull(); } else { out.writeNumber(Math.nextUp(1.123456789012345 + i)); }
                        out.writeNumber(100 - i); out.writeNumber(-i);
                    }
                    out.writeEndArray(); out.writeEndObject();
                }
                redis.jsonSet("json_topn_paths:" + i, JsonPath.ROOT_PATH, text.toString());
            }
            server.awaitIndexed("json_topn_paths");
            var args = io.lettuce.core.search.arguments.AggregateArgs.builder()
                    .dialect(io.lettuce.core.search.arguments.QueryDialects.DIALECT3).load("@__key");
            paths.keySet().forEach(alias -> args.load("@" + alias));
            var reply = redis.ftAggregate("json_topn_paths", "*", args.build());
            RediSearchQueryErrors.verifyComplete(reply);
            int rows = 0;
            for (var shard : reply.getReplies()) {
                for (var row : shard.getResults()) {
                    rows++;
                    String key = row.getFields().get("__key").asString();
                    var command = new io.lettuce.core.protocol.CommandArgs<>(io.lettuce.core.codec.StringCodec.UTF8)
                            .addKey(key).addValues(paths.values());
                    String raw = redis.dispatch(io.lettuce.core.protocol.CommandType.JSON_GET,
                            new io.lettuce.core.output.ValueOutput<>(io.lettuce.core.codec.StringCodec.UTF8), command);
                    Map<String, String> expected = new java.util.LinkedHashMap<>();
                    for (var path : paths.entrySet()) {
                        var loaded = row.getFields().get(path.getKey());
                        String value = loaded == null || loaded.isNull() ? null
                                : RediSearchRowReader.firstValue(loaded.asString()).orElse(null);
                        expected.put(path.getValue(), value);
                        String single = redis.dispatch(io.lettuce.core.protocol.CommandType.JSON_GET,
                                new io.lettuce.core.output.ValueOutput<>(io.lettuce.core.codec.StringCodec.UTF8),
                                new io.lettuce.core.protocol.CommandArgs<>(io.lettuce.core.codec.StringCodec.UTF8).addKey(key).addValue(path.getValue()));
                        assertThat(RediSearchJsonValues.decode(single, List.of(path.getValue())).get(path.getValue()))
                                .as("%s path %s single reply %s", key, path.getValue(), single).isEqualTo(value);
                    }
                    try { assertThat(RediSearchJsonValues.decode(raw, List.copyOf(paths.values()))).isEqualTo(expected); }
                    catch (RuntimeException failure) { throw new AssertionError("JSON.GET reply " + raw, failure); }
                }
            }
            assertThat(rows).isEqualTo(17);
            for (String order : List.of("space ASC NULLS FIRST", "wild DESC NULLS LAST", "nums DESC NULLS FIRST",
                    "recursive_value ASC NULLS LAST", "item DESC NULLS LAST")) {
                String sql = "SELECT id, space, dotted, unicode, quote, slash, members, item, last_item, wild, slice, stepped, recursive_value, object_leaf, nums, \"$\" "
                        + "FROM json_topn_paths ORDER BY " + order + ", id ASC, __key ASC LIMIT 5";
                assertExactOrder(sql);
                Metrics selected = metrics(sql);
                assertThat(metric(selected, "redis.rows.received")).isEqualTo(17);
                assertThat(metric(selected, "redis.local-topn.candidates")).isEqualTo(5);
                assertThat(metric(selected, "redis.json.commands")).isEqualTo(5);
                assertThat(metric(selected, "redis.local-topn.projected-json-reads")).isEqualTo(80);
            }
        } finally { assertUpdate("DROP TABLE json_topn_paths"); }
    }

    @Test
    public void testWorkerRefreshesNewSortAndProjectionColumnsOnlyOnCacheMiss() {
        var redis = server.getConnection().sync();
        redis.ftCreate("json_topn_cached", CreateArgs.builder().withPrefix("json_topn_cached:").skipInitialScan().build(), List.of(
                NumericFieldArgs.builder().name("raw_id").as("id").build()));
        redis.hset("json_topn_cached:1", Map.of("raw_id", "1"));
        redis.hset("json_topn_cached:2", Map.of("raw_id", "2"));
        server.awaitIndexed("json_topn_cached");
        var types = getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager();
        var tableName = new SchemaTableName("tpch", "json_topn_cached");
        var refreshed = new java.util.concurrent.atomic.AtomicInteger();
        var worker = new RediSearchSession(types, directConfig().setTableCacheRefresh(3600)) {
            @Override
            RediSearchTable refreshTable(SchemaTableName name) {
                refreshed.incrementAndGet();
                return super.refreshTable(name);
            }
        };
        var coordinator = new RediSearchSession(types, directConfig().setTableCacheRefresh(3600));
        try (var reader = worker.exactHashReader()) {
            var cached = worker.getTable(tableName);
            var oldId = cached.getColumns().stream().filter(column -> column.getName().equals("id")).findFirst().orElseThrow();
            RediSearchLocalTopN.read(worker, worker.scanConnection(), cached.getTableHandle()
                    .withTopN(List.of(new RediSearchSortItem("id", true, true)), 1), List.of(oldId), reader, new RediSearchReadStats());
            assertThat(refreshed.get()).isZero();
            assertUpdate("ALTER TABLE json_topn_cached ADD COLUMN added varchar FIRST");
            redis.hset("json_topn_cached:1", "added", "z");
            redis.hset("json_topn_cached:2", "added", "a");
            var schema = coordinator.getTable(tableName);
            var outputs = schema.getColumns().stream().filter(column -> List.of("id", "added").contains(column.getName())).toList();
            var projectionOnly = schema.getTableHandle().withTopN(List.of(new RediSearchSortItem("id", true, true)), 1);
            var projected = RediSearchLocalTopN.read(worker, worker.scanConnection(), projectionOnly, outputs, reader, new RediSearchReadStats());
            assertThat(projected.getRows().getFirst()).containsExactly("z", "1");
            assertThat(refreshed.get()).isEqualTo(1);
            var topN = schema.getTableHandle().withTopN(List.of(new RediSearchSortItem("added", true, true)), 1);
            for (int repeat = 0; repeat < 2; repeat++) {
                var result = RediSearchLocalTopN.read(worker, worker.scanConnection(), topN, outputs, reader, new RediSearchReadStats());
                assertThat(result.getRows()).hasSize(1);
                assertThat(result.getRows().getFirst()).containsExactly("a", "2");
                assertThat(refreshed.get()).isEqualTo(1);
            }
            var sampled = new RediSearchColumnHandle("sampled", io.trino.spi.type.VarcharType.VARCHAR,
                    RediSearchFieldType.TEXT, false, false, Optional.empty());
            var sampledOrder = topN.withoutTopN().withTopN(List.of(new RediSearchSortItem("sampled", true, true)), 1);
            var sampledResult = RediSearchLocalTopN.read(worker, worker.scanConnection(), sampledOrder,
                    List.of(sampled, oldId), reader, new RediSearchReadStats());
            assertThat(sampledResult.getRows()).hasSize(1);
            assertThat(sampledResult.getRows().getFirst()[0]).isNull();
            assertThat(refreshed.get()).isEqualTo(2);
        } finally { worker.shutdown(); coordinator.shutdown(); assertUpdate("DROP TABLE json_topn_cached"); }
    }

    @Test
    public void testMultiMatchNumericWhereUsesFirstMatchSqlSemantics() {
        var redis = server.getConnection().sync();
        redis.ftCreate("json_topn_multi", CreateArgs.builder().on(CreateArgs.TargetType.JSON)
                .withPrefix("json_topn_multi:").skipInitialScan().build(), List.of(
                        NumericFieldArgs.builder().name("$.id").as("id").build(),
                        NumericFieldArgs.builder().name("$.items[*].rank").as("rank").build()));
        try {
            redis.jsonSet("json_topn_multi:1", JsonPath.ROOT_PATH, "{\"id\":1,\"items\":[{\"rank\":1},{\"rank\":99}]}");
            redis.jsonSet("json_topn_multi:2", JsonPath.ROOT_PATH, "{\"id\":2,\"items\":[{\"rank\":99},{\"rank\":1}]}");
            redis.jsonSet("json_topn_multi:3", JsonPath.ROOT_PATH, "{\"id\":3,\"items\":[{\"rank\":null},{\"rank\":99}]}");
            redis.jsonSet("json_topn_multi:4", JsonPath.ROOT_PATH, "{\"id\":4,\"items\":[{},{\"rank\":99}]}");
            redis.jsonSet("json_topn_multi:5", JsonPath.ROOT_PATH, "{\"id\":5,\"items\":[]}");
            server.awaitIndexed("json_topn_multi");
            // Literal expectations prevent two connector catalogs from sharing the same consumed-predicate bug.
            assertThat(query("SELECT id FROM json_topn_multi WHERE rank > 50 ORDER BY id"))
                    .matches("VALUES DOUBLE '2', DOUBLE '4'");
            assertThat(query("SELECT count(*) FROM json_topn_multi WHERE rank > 50"))
                    .matches("VALUES BIGINT '2'");
            assertThat(query("SELECT id FROM json_topn_multi WHERE rank > 50 ORDER BY rank DESC, id ASC LIMIT 1"))
                    .matches("VALUES DOUBLE '2'");
            assertExactOrder("SELECT id, rank FROM json_topn_multi WHERE rank > 50 ORDER BY rank DESC, id ASC LIMIT 1");
            assertThat(query("SELECT id FROM json_topn_multi WHERE rank IS NULL ORDER BY id"))
                    .matches("VALUES DOUBLE '3', DOUBLE '5'");
            assertThat(query("SELECT id FROM json_topn_multi WHERE rank IS NOT NULL ORDER BY id"))
                    .matches("VALUES DOUBLE '1', DOUBLE '2', DOUBLE '4'");
            assertThat(query("SELECT id FROM json_topn_multi WHERE rank < 0 OR rank IS NULL ORDER BY id"))
                    .matches("VALUES DOUBLE '3', DOUBLE '5'");
            assertThat(query("SELECT id FROM json_topn_multi WHERE rank > 50 OR rank IS NULL ORDER BY id"))
                    .matches("VALUES DOUBLE '2', DOUBLE '3', DOUBLE '4', DOUBLE '5'");
        } finally { assertUpdate("DROP TABLE json_topn_multi"); }
    }

    @Test
    public void testLegacyAndInvalidModernPathSchemasRetainFallback() {
        var redis = server.getConnection().sync();
        int suffix = 0;
        for (String path : List.of("plain", "$.café", "$.items[::-1]")) {
            String index = "json_topn_legacy" + suffix++;
            redis.ftCreate(index, CreateArgs.builder().on(CreateArgs.TargetType.JSON).withPrefix(index + ":")
                    .skipInitialScan().build(), List.of(NumericFieldArgs.builder().name("$.id").as("id").build(),
                            TagFieldArgs.builder().name(path).as("unsupported").build()));
            try {
                redis.jsonSet(index + ":1", JsonPath.ROOT_PATH, "{\"id\":1,\"plain\":\"legacy\",\"café\":\"unicode\",\"items\":[\"a\",\"b\"]}");
                redis.jsonSet(index + ":2", JsonPath.ROOT_PATH, "{\"id\":2,\"plain\":\"legacy\",\"café\":\"unicode\",\"items\":[\"a\",\"b\"]}");
                server.awaitIndexed(index);
                String sql = "SELECT id FROM " + index + " ORDER BY id DESC LIMIT 1";
                assertThat(query(sql)).matches("VALUES DOUBLE '2'");
                assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isZero();
                assertThat(metric(metrics(sql), "redis.json.commands")).isZero();
            } finally { assertUpdate("DROP TABLE " + index); }
        }
    }

    @Test
    public void testScalarNumericPathArrayValuesCannotHideTypeErrors() {
        var redis = server.getConnection().sync();
        redis.ftCreate("json_topn_scalar_array", CreateArgs.builder().on(CreateArgs.TargetType.JSON)
                .withPrefix("json_topn_scalar_array:").skipInitialScan().build(), List.of(
                        NumericFieldArgs.builder().name("$.id").as("id").build(),
                        NumericFieldArgs.builder().name("$.rank").as("rank").build()));
        try {
            redis.jsonSet("json_topn_scalar_array:1", JsonPath.ROOT_PATH, "{\"id\":1,\"rank\":[1,99]}");
            redis.jsonSet("json_topn_scalar_array:2", JsonPath.ROOT_PATH, "{\"id\":2,\"rank\":[99,1]}");
            server.awaitIndexed("json_topn_scalar_array");
            // A singular selector can match an ARRAY node; its JSON text is invalid for the SQL DOUBLE column.
            assertThatThrownBy(() -> computeActual("SELECT rank FROM json_topn_scalar_array"))
                    .hasMessageContaining("For input string: \"[");
            for (String sql : List.of("SELECT id FROM json_topn_scalar_array WHERE rank > 50",
                    "SELECT count(*) FROM json_topn_scalar_array WHERE rank > 50",
                    "SELECT id FROM json_topn_scalar_array WHERE rank > 50 ORDER BY id LIMIT 1")) {
                assertThatThrownBy(() -> computeActual(sql)).as(sql).hasMessageContaining("For input string: \"[");
            }
        } finally { assertUpdate("DROP TABLE json_topn_scalar_array"); }
    }

    @Test
    public void testScalarNumericWhereRetainsNativeSupersetAndSqlResidual() {
        String predicate = "rank > DOUBLE '0.123456789012346'";
        // Decode the full source without a predicate as an independent oracle. Redis JSON can serialize a
        // boundary-adjacent double differently from the original Java fixture, so compare actual SQL values.
        long expected = computeActual("SELECT rank FROM json_topn").getMaterializedRows().stream()
                .map(row -> row.getField(0)).filter(java.util.Objects::nonNull).map(Double.class::cast)
                .filter(rank -> rank > 0.123456789012346).count();
        assertThat(query("SELECT count(*) FROM json_topn WHERE " + predicate)).matches("VALUES BIGINT '" + expected + "'");
        String sql = "SELECT id, rank FROM json_topn WHERE " + predicate + " ORDER BY rank DESC, id ASC LIMIT 5";
        assertExactOrder(sql);
        Metrics selected = metrics(sql);
        long received = metric(selected, "redis.rows.received");
        assertThat(received).isGreaterThanOrEqualTo(expected).isLessThan(ROWS);
        assertThat(metric(selected, "redis.local-topn.candidates")).isZero();
        System.out.println("JSON_NUMERIC_PREFILTER resp2=" + resp2() + " decodedMatches=" + expected + " nativeSource=" + received);
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
    public void testComputedResidualFiltersBeforeJsonTopNPruning() {
        String sql = "SELECT id, rank, payload FROM json_topn WHERE mod(id, BIGINT '7') = BIGINT '0' "
                + "ORDER BY rank DESC NULLS FIRST, id ASC LIMIT 5";
        assertExactOrder(sql);
        // The highest ranked unfiltered rows include nonmatches; pruning before this residual loses winners.
        assertThat(computeActual(sql).getRowCount()).isEqualTo(5);
        assertThat(computeActual(sql).getMaterializedRows().stream().map(row -> (Long) row.getField(0)))
                .allMatch(id -> id % 7 == 0);
        Metrics selected = metrics(sql);
        assertThat(metric(selected, "redis.rows.received")).isEqualTo(ROWS);
        assertThat(metric(selected, "redis.local-topn.candidates")).isZero();
        assertThat(metric(selected, "redis.local-topn.projected-json-reads")).isZero();
    }

    @Test
    public void testWildcardSchemaUsesExactFirstMatchTopN() {
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
            assertThat(metric(metrics(sql), "redis.local-topn.candidates")).isEqualTo(2);
            assertThat(metric(metrics(sql), "redis.json.commands")).isEqualTo(2);
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
