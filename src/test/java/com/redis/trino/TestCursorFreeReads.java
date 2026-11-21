package com.redis.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/** The same reader contract is exercised on RAM and, by TestFlexReads, actual Software Flex. */
public class TestCursorFreeReads extends AbstractTestQueryFramework {
    protected RediSearchServer server;
    protected Deployment deployment() { return Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.flex.enabled", "true", "redisearch.flex.batch-size", "37"));
    }

    private void hashIndex(String index) {
        server.getConnection().sync().ftCreate(index, CreateArgs.builder().withPrefix(index + ":").skipInitialScan().build(),
                List.of(NumericFieldArgs.builder().name("raw_id").as("id").build(),
                        NumericFieldArgs.builder().name("amount").build(), TagFieldArgs.builder().name("note").build()));
        RediSearchColumnTypes.write(server.getConnection().sync(), index, Map.of("id", BIGINT, "amount", DOUBLE, "note", VARCHAR));
    }

    @Test
    public void testNativeSearchUsesCursorFreePredicateAndSqlResidual() {
        String index = "cf_native";
        hashIndex(index);
        var redis = server.getConnection().sync();
        redis.hset(index + ":1", Map.of("raw_id", "1", "amount", "2", "note", "GroupA"));
        redis.hset(index + ":2", Map.of("raw_id", "2", "amount", "3", "note", "GroupB"));
        redis.hset(index + ":3", Map.of("raw_id", "3", "amount", "4", "note", "GroupA"));
        server.awaitIndexed(index);
        assertThat(query("SELECT id FROM TABLE(redisearch.system.search('cf_native', '@note:{GroupA}')) WHERE id > 1"))
                .matches("VALUES BIGINT '3'");
        assertThat(query("SELECT count(*) FROM TABLE(redisearch.system.search('cf_native', '@note:{GroupA}'))"))
                .matches("VALUES BIGINT '2'");
        assertQueryFails("SELECT * FROM TABLE(redisearch.system.search('cf_native', '@note:('))", ".*(Syntax|syntax).*" );
    }

    @Test
    public void testMultiplePagesWithExactNumbersNullsAndSqlOperations() {
        String index = "cf_rows";
        hashIndex(index);
        for (int i = 0; i < 1103; i++) {
            Map<String, String> fields = new HashMap<>(Map.of("raw_id", Long.toString(9007199254740993L + i),
                    "amount", "0.1234567890123456"));
            if (i % 2 == 0) { fields.put("note", "GroupA"); }
            server.getConnection().sync().hset(index + ":" + i, fields);
        }
        server.awaitIndexed(index);
        var rows = computeActual("SELECT id, amount, note FROM " + index).getMaterializedRows();
        assertThat(rows).hasSize(1103);
        assertThat(rows.stream().map(row -> (Long) row.getField(0)).sorted().toList())
                .containsExactlyElementsOf(java.util.stream.LongStream.range(9007199254740993L, 9007199254742096L).boxed().toList());
        rows.forEach(row -> {
            assertThat(row.getField(1)).isEqualTo(0.1234567890123456);
            assertThat(row.getField(2)).isEqualTo(((Long) row.getField(0) - 9007199254740993L) % 2 == 0 ? "GroupA" : null);
        });
        assertThat(query("SELECT count(*) FROM " + index)).matches("VALUES BIGINT '1103'");
        assertThat(query("SELECT count(*) FROM " + index + " WHERE note LIKE 'Group%'")).matches("VALUES BIGINT '552'");
        assertThat(query("SELECT count(*) FROM " + index + " WHERE note = 'groupa'")).matches("VALUES BIGINT '0'");
        assertThat(query("SELECT note, count(*) FROM " + index + " GROUP BY note"))
                .matches("VALUES (VARCHAR 'GroupA', BIGINT '552'), (CAST(NULL AS VARCHAR), BIGINT '551')");
        assertThat(query("SELECT id FROM " + index + " WHERE id > 9007199254742090 ORDER BY id DESC LIMIT 3"))
                .matches("VALUES BIGINT '9007199254742095', BIGINT '9007199254742094', BIGINT '9007199254742093'");
        assertThat(query("SELECT amount * 2 FROM " + index + " LIMIT 1")).matches("VALUES DOUBLE '0.2469135780246912'");
    }

    @Test
    public void testQuotedUnicodeKeysAndShortBatches() {
        String index = "cf_keys";
        hashIndex(index);
        List<String> suffixes = List.of("Z", "a\"b", "a\\b", "A", "z", "é", "", "😀", "1", "10", "2", "terminal\\", "slash\\ space", "repeat\\\\b");
        for (int i = 0; i < suffixes.size(); i++) {
            server.getConnection().sync().hset(index + ":" + suffixes.get(i), Map.of("raw_id", "" + i, "amount", "1.0"));
        }
        server.awaitIndexed(index);
        AtomicInteger requests = new AtomicInteger();
        // Simulate a server returning fewer rows than requested, without assuming that means end-of-results.
        RediSearchSession session = new RediSearchSession(
                getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager(), config()) {
            @Override
            CompletableFuture<AggregationReply<String>> aggregateAsync(Connection scan,
                    RediSearchTranslator.Aggregation aggregation, RediSearchReadStats stats) {
                requests.incrementAndGet();
                return super.aggregateAsync(scan, aggregation, stats).thenApply(reply -> {
                    var shortened = new AggregationReply<String>();
                    for (var shard : reply.getReplies()) {
                        var page = new io.lettuce.core.search.SearchReply<String>();
                        page.getWarnings().addAll(shard.getWarnings());
                        if (!shard.isEmpty()) { page.addResult(shard.getResults().get(0)); }
                        shortened.getReplies().add(page);
                    }
                    return shortened;
                });
            }
        };
        try (var source = new RediSearchCursorFreePageSource(session, table(index),
                List.of(RediSearchBuiltinField.KEY.getColumnHandle()))) {
            List<String> keys = new java.util.ArrayList<>();
            while (!source.isFinished()) {
                source.isBlocked().join();
                var page = source.getNextSourcePage();
                if (page != null) {
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        keys.add(VARCHAR.getSlice(page.getBlock(0), row).toStringUtf8());
                    }
                }
            }
            assertThat(keys).containsExactlyInAnyOrderElementsOf(suffixes.stream().map(suffix -> index + ":" + suffix).toList());
            assertThat(requests.get()).isEqualTo(suffixes.size() + 1);
            assertThat(((LongCount) source.getMetrics().getMetrics().get("redis.cursor.requests")).getTotal()).isZero();
            assertThat(source.getCompletedBytes()).isPositive();
        } finally { session.shutdown(); }
    }

    @Test
    public void testEmptyAndJson() {
        hashIndex("cf_empty");
        assertThat(query("SELECT count(*) FROM cf_empty")).matches("VALUES BIGINT '0'");
        assertThat(query("SELECT sum(id) FROM cf_empty")).matches("VALUES CAST(NULL AS BIGINT)");
        assertQueryFails("SELECT * FROM cf_missing_table", ".*does not exist.*");
        var redis = server.getConnection().sync();
        redis.ftCreate("cf_json", CreateArgs.builder().withPrefix("cf_json:").on(CreateArgs.TargetType.JSON).skipInitialScan().build(),
                List.of(NumericFieldArgs.builder().name("$.n.id").as("id").build(),
                        TagFieldArgs.builder().name("$.note").as("note").build()));
        for (int i = 0; i < 83; i++) {
            redis.jsonSet("cf_json:" + i, JsonPath.ROOT_PATH, "{\"n\":{\"id\":" + (9007199254740993L + i) + "},\"note\":\"hello\"}");
        }
        RediSearchColumnTypes.write(redis, "cf_json", Map.of("id", BIGINT, "note", VARCHAR));
        server.awaitIndexed("cf_json");
        assertThat(query("SELECT count(*) FROM cf_json WHERE id > 9007199254740993")).matches("VALUES BIGINT '82'");

    }

    private RediSearchConfig config() {
        return new RediSearchConfig().setUri(server.getRedisURI()).setFlexEnabled(true).setFlexBatchSize(37);
    }

    private RediSearchTableHandle table(String index) {
        return new RediSearchTableHandle(new SchemaTableName("tpch", index), index);
    }

    @Test
    public void testCancellationDoesNotStartAnotherRequest() {
        hashIndex("cf_cancel");
        AtomicInteger requests = new AtomicInteger();
        CompletableFuture<AggregationReply<String>> waiting = new CompletableFuture<>();
        RediSearchSession session = new RediSearchSession(
                getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager(), config()) {
            @Override
            CompletableFuture<AggregationReply<String>> aggregateAsync(Connection scan,
                    RediSearchTranslator.Aggregation aggregation, RediSearchReadStats stats) {
                requests.incrementAndGet();
                return waiting;
            }
        };
        try {
            var source = new RediSearchCursorFreePageSource(session, table("cf_cancel"), List.of());
            assertThat(source.isBlocked().isDone()).isFalse();
            source.close();
            assertThat(source.isFinished()).isTrue();
            assertThat(source.getNextSourcePage()).isNull();
            assertThat(waiting.isCancelled()).isTrue();
            assertThat(requests.get()).isEqualTo(1);
        } finally { session.shutdown(); }
    }

    @Test
    public void testLaterWarningsFailTheRead() {
        hashIndex("cf_warning");
        for (int i = 0; i < 80; i++) {
            server.getConnection().sync().hset("cf_warning:" + i, Map.of("raw_id", "" + i));
        }
        server.awaitIndexed("cf_warning");
        AtomicInteger requests = new AtomicInteger();
        RediSearchSession session = new RediSearchSession(
                getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager(), config()) {
            @Override
            CompletableFuture<AggregationReply<String>> aggregateAsync(Connection scan,
                    RediSearchTranslator.Aggregation aggregation, RediSearchReadStats stats) {
                int request = requests.incrementAndGet();
                return super.aggregateAsync(scan, aggregation, stats).thenApply(reply -> {
                    if (request == 2) { reply.getReplies().get(0).getWarnings().add("Timeout limit was reached"); }
                    return reply;
                });
            }
        };
        try (var source = new RediSearchCursorFreePageSource(session, table("cf_warning"), List.of())) {
            assertThatThrownBy(() -> {
                while (!source.isFinished()) { source.isBlocked().join(); source.getNextSourcePage(); }
            }).isInstanceOfSatisfying(TrinoException.class, error -> assertThat(error.getErrorCode())
                    .isEqualTo(RediSearchErrorCode.REDISEARCH_INCOMPLETE_RESULT.toErrorCode()));
            assertThat(source.isFinished()).isTrue();
        } finally { session.shutdown(); }
    }

    @Test
    public void testLaterFailureIsPreservedWithoutRetry() {
        hashIndex("cf_failure");
        server.getConnection().sync().hset("cf_failure:1", Map.of("raw_id", "1"));
        AtomicInteger requests = new AtomicInteger();
        RediSearchSession session = new RediSearchSession(
                getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager(), config()) {
            @Override
            CompletableFuture<AggregationReply<String>> aggregateAsync(Connection scan,
                    RediSearchTranslator.Aggregation aggregation, RediSearchReadStats stats) {
                if (requests.incrementAndGet() == 2) {
                    return CompletableFuture.failedFuture(new io.lettuce.core.RedisCommandExecutionException("NOPERM blocked read"));
                }
                return super.aggregateAsync(scan, aggregation, stats);
            }
        };
        try (var source = new RediSearchCursorFreePageSource(session, table("cf_failure"), List.of())) {
            assertThatThrownBy(() -> {
                while (!source.isFinished()) { source.isBlocked().join(); source.getNextSourcePage(); }
            }).isInstanceOf(io.lettuce.core.RedisCommandExecutionException.class).hasMessageContaining("NOPERM");
            assertThat(source.isFinished()).isTrue();
            assertThat(requests.get()).isEqualTo(2);
        } finally { session.shutdown(); }
    }
}
