package com.redis.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static io.trino.testing.assertions.Assert.assertEventually;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import io.airlift.slice.Slice;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.spi.Page;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

class TestFlexWrites extends AbstractTestQueryFramework {
    private RediSearchServer server;

    @Test
    void testTruncatePreservesFlexSchemaAndComments() {
        assertUpdate("CREATE TABLE fw_truncate (id bigint, note varchar) COMMENT 'retained'");
        try {
            assertUpdate("INSERT INTO fw_truncate SELECT n, 'value' FROM UNNEST(sequence(1, 41)) t(n)", 41);
            assertUpdate("TRUNCATE TABLE fw_truncate");
            assertThat(query("SELECT count(*) FROM fw_truncate")).matches("VALUES BIGINT '0'");
            assertThat(computeActual("SHOW CREATE TABLE fw_truncate").getOnlyValue().toString()).contains("retained");
            assertUpdate("INSERT INTO fw_truncate VALUES (7, 'again')", 1);
            assertThat(query("SELECT id FROM fw_truncate")).matches("VALUES BIGINT '7'");
        } finally { assertUpdate("DROP TABLE fw_truncate"); }
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(RedisEnterprise.Deployment.FLEX));
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of("query.max-run-time", "1m"),
                Map.of("redisearch.flex.enabled", "true", "redisearch.flex.batch-size", "13"));
    }

    @Test
    void testBulkInsertUpdateDeleteAndMerge() {
        assertUpdate("CREATE TABLE fw_bulk (id bigint, amount double, note varchar)");
        try {
            assertUpdate("INSERT INTO fw_bulk SELECT 9007199254740993 + n, DOUBLE '0.1234567890123456', 'initial' "
                    + "FROM UNNEST(sequence(BIGINT '0', BIGINT '1102')) t(n)", 1103);
            assertThat(query("SELECT count(*), min(id), max(id) FROM fw_bulk"))
                    .matches("VALUES (BIGINT '1103', BIGINT '9007199254740993', BIGINT '9007199254742095')");
            assertThat(computeActual("SELECT DISTINCT amount FROM fw_bulk").getOnlyValue()).isEqualTo(0.1234567890123456);
            assertUpdate("UPDATE fw_bulk SET note = 'updated' WHERE id > 9007199254740993", 1102);
            assertThat(query("SELECT count(*) FROM fw_bulk WHERE note = 'updated'")).matches("VALUES BIGINT '1102'");
            assertUpdate("MERGE INTO fw_bulk t USING (VALUES (BIGINT '9007199254740993', 'merged'), (BIGINT '1', 'inserted')) s(id, note) "
                    + "ON t.id = s.id WHEN MATCHED THEN UPDATE SET note = s.note "
                    + "WHEN NOT MATCHED THEN INSERT (id, note) VALUES (s.id, s.note)", 2);
            assertThat(query("SELECT id, note FROM fw_bulk WHERE id IN (1, 9007199254740993)"))
                    .matches("VALUES (BIGINT '1', VARCHAR 'inserted'), (BIGINT '9007199254740993', VARCHAR 'merged')");
            assertUpdate("DELETE FROM fw_bulk WHERE id > 1", 1103);
            assertThat(query("SELECT count(*) FROM fw_bulk")).matches("VALUES BIGINT '1'");
        } finally { assertUpdate("DROP TABLE fw_bulk"); }
    }

    @Test
    void testInsertSelectDoesNotReadItsOwnInserts() {
        assertUpdate("CREATE TABLE fw_self (id bigint)");
        try {
            var redis = server.getConnection().sync();
            // Generated ULID keys sort between these low keys and the final 'z' key. Streaming inserts would be
            // picked up by later keyset requests, even if the reader captured the original maximum key.
            for (int i = 0; i < 30; i++) { redis.hset("fw_self:000" + i, Map.of("id", "" + i)); }
            redis.hset("fw_self:z", Map.of("id", "30"));
            assertUpdate("INSERT INTO fw_self SELECT id + 100 FROM fw_self", 31);
            assertThat(query("SELECT count(*), count(DISTINCT id), sum(id) FROM fw_self"))
                    .matches("VALUES (BIGINT '62', BIGINT '62', BIGINT '4030')");
            assertUpdate("MERGE INTO fw_self t USING (SELECT id + 200 id FROM fw_self) s ON t.id = s.id "
                    + "WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)", 62);
            assertThat(query("SELECT count(*), count(DISTINCT id) FROM fw_self")).matches("VALUES (BIGINT '124', BIGINT '124')");
        } finally { assertUpdate("DROP TABLE fw_self"); }
    }

    @Test
    void testNullRowsAndLogicalTypedAddColumn() {
        assertUpdate("CREATE TABLE fw_nulls (id bigint)");
        try {
            assertUpdate("INSERT INTO fw_nulls VALUES (NULL), (2)", 2);
            assertUpdate("UPDATE fw_nulls SET id = NULL WHERE id = 2", 1);
            assertThat(query("SELECT count(*) FROM fw_nulls WHERE id IS NULL")).matches("VALUES BIGINT '2'");
            assertUpdate("ALTER TABLE fw_nulls ADD COLUMN extra decimal(30,5)");
            assertThat(query("SELECT extra FROM fw_nulls")).matches("VALUES CAST(NULL AS decimal(30,5)), CAST(NULL AS decimal(30,5))");
            assertUpdate("UPDATE fw_nulls SET extra = DECIMAL '1234567890123456789012345.12345'", 2);
            assertThat(query("SELECT DISTINCT extra FROM fw_nulls")).matches("VALUES DECIMAL '1234567890123456789012345.12345'");
            assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_name = 'fw_nulls' AND table_schema = 'tpch'"))
                    .matches("VALUES VARCHAR 'id', VARCHAR 'extra'");
            assertQueryFails("ALTER TABLE fw_nulls ADD COLUMN __trino_row_marker varchar", ".*Reserved Flex column name.*");
        } finally { assertUpdate("DROP TABLE fw_nulls"); }
        assertQueryFails("CREATE TABLE fw_reserved (__trino_row_marker varchar)", ".*Reserved Flex column name.*");
    }

    @Test
    void testDropDeletesOnlyIndexedDocuments() {
        var redis = server.getConnection().sync();
        redis.ftCreate("fw_drop", CreateArgs.builder().withPrefix("fw_drop:").filter("@keep == 'yes'").skipInitialScan().build(),
                List.of(TagFieldArgs.builder().name("id").build()));
        redis.hset("fw_drop:in", Map.of("id", "in", "keep", "yes"));
        redis.hset("fw_drop:out", Map.of("id", "out", "keep", "no"));
        try {
            assertThat(query("SELECT id FROM fw_drop")).matches("VALUES VARCHAR 'in'");
            assertUpdate("DROP TABLE fw_drop");
            assertThat(redis.exists("fw_drop:in")).isZero();
            assertThat(redis.exists("fw_drop:out")).isEqualTo(1);
            assertThat(redis.ftList()).doesNotContain("fw_drop");
        } finally { redis.del("fw_drop:out"); }
    }

    @Test
    void testWritesRespectExternalHashAliases() {
        var redis = server.getConnection().sync();
        redis.ftCreate("fw_alias", CreateArgs.builder().withPrefix("fw_alias:").skipInitialScan().build(),
                List.of(NumericFieldArgs.builder().name("raw_id").as("id").build(),
                        TagFieldArgs.builder().name("raw_note").as("note").build()));
        RediSearchColumnTypes.write(redis, "fw_alias", Map.of("id", BIGINT, "note", VARCHAR));
        try {
            assertUpdate("INSERT INTO fw_alias VALUES (9007199254740993, 'hello')", 1);
            assertUpdate("UPDATE fw_alias SET id = id + 1, note = NULL", 1);
            assertThat(query("SELECT id, note FROM fw_alias"))
                    .matches("VALUES (BIGINT '9007199254740994', CAST(NULL AS varchar))");
            String key = (String) computeActual("SELECT __key FROM fw_alias").getOnlyValue();
            assertThat(redis.hgetall(key)).containsEntry("raw_id", "9007199254740994").doesNotContainKeys("id", "note", "raw_note");
        } finally { assertUpdate("DROP TABLE fw_alias"); }
    }

    @Test
    void testLiteralPrefixIsNotAGlob() {
        var redis = server.getConnection().sync();
        redis.ftCreate("fw_star", CreateArgs.builder().withPrefix("*").skipInitialScan().build(),
                List.of(TagFieldArgs.builder().name("id").build()));
        try {
            assertUpdate("INSERT INTO fw_star VALUES ('star')", 1);
            assertThat(query("SELECT id FROM fw_star")).matches("VALUES VARCHAR 'star'");
            assertThat((String) computeActual("SELECT __key FROM fw_star").getOnlyValue()).startsWith("*:");
        } finally { assertUpdate("DROP TABLE fw_star"); }
    }

    @Test
    void testJsonDeletionAndHashOnlyMutationGuards() {
        var redis = server.getConnection().sync();
        redis.ftCreate("fw_json", CreateArgs.builder().withPrefix("fw_json:").on(CreateArgs.TargetType.JSON).skipInitialScan().build(),
                List.of(NumericFieldArgs.builder().name("$.id").as("id").build()));
        redis.jsonSet("fw_json:1", JsonPath.ROOT_PATH, "{\"id\":1}");
        redis.jsonSet("fw_json:2", JsonPath.ROOT_PATH, "{\"id\":2}");
        try {
            assertUpdate("DELETE FROM fw_json WHERE id = 1", 1);
            assertQueryFails("INSERT INTO fw_json (id) VALUES (3)", ".*only writes hashes.*");
            assertQueryFails("UPDATE fw_json SET id = 3", ".*only writes hashes.*");
            assertQueryFails("ALTER TABLE fw_json ADD COLUMN x bigint", ".*only writes hashes.*");
            assertThat(query("SELECT id FROM fw_json")).matches("VALUES DOUBLE '2'");
        } finally { assertUpdate("DROP TABLE fw_json"); }
        assertThat(redis.exists("fw_json:2")).isZero();
    }

    @Test
    void testCtasAndFailedCtasCleanup() {
        assertUpdate("CREATE TABLE fw_ctas AS SELECT BIGINT '9007199254740993' id, true flag, DATE '2026-11-21' day", 1);
        try {
            assertThat(query("SELECT * FROM fw_ctas")).matches("VALUES (BIGINT '9007199254740993', true, DATE '2026-11-21')");
        } finally { assertUpdate("DROP TABLE fw_ctas"); }
        assertQueryFails("CREATE TABLE fw_bad_ctas AS SELECT n, BIGINT '10' / (n - 2500) quotient "
                + "FROM UNNEST(sequence(BIGINT '1', BIGINT '3000')) t(n)", ".*Division by zero.*");
        assertEventually(() -> {
            assertThat(server.getConnection().sync().ftList()).doesNotContain("fw_bad_ctas");
            assertThat(server.getConnection().sync().exists(RediSearchColumnTypes.key("fw_bad_ctas"))).isZero();
        });
    }

    @Test
    void testStagingVisibilityAbortAndMissingDataFailClosed() {
        assertUpdate("CREATE TABLE fw_stage (id bigint)");
        var types = getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager();
        var session = new RediSearchSession(types, new RediSearchConfig().setUri(server.getRedisURI()).setFlexEnabled(true));
        try {
            var table = new SchemaTableName("tpch", "fw_stage");
            var columns = List.of(session.getTable(table).getColumns().get(0));
            var block = BIGINT.createFixedSizeBlockBuilder(2);
            BIGINT.writeLong(block, 1);
            BIGINT.writeLong(block, 2);
            var page = new Page(block.build());
            var sink = new RediSearchPageSink(session, table, columns);
            sink.appendPage(page).join();
            List<Slice> fragments = List.copyOf(sink.finish().join());
            String key = RediSearchFlexWriteBuffer.ROW_CODEC.fromJson(fragments.get(0).toStringUtf8()).get(0);
            var redis = server.getConnection().sync();
            assertThat(redis.type(key)).isEqualTo("list");
            assertThat(redis.ttl(key)).isPositive();
            assertThat(query("SELECT count(*) FROM fw_stage")).matches("VALUES BIGINT '0'");
            session.publishFlexStaging(fragments);
            assertThat(redis.exists(key)).isZero();
            assertThat(query("SELECT id FROM fw_stage")).matches("VALUES BIGINT '1', BIGINT '2'");

            var aborted = new RediSearchPageSink(session, table, columns);
            aborted.appendPage(page).join();
            var lost = List.copyOf(aborted.finish().join());
            String lostKey = RediSearchFlexWriteBuffer.ROW_CODEC.fromJson(lost.get(0).toStringUtf8()).get(0);
            aborted.abort();
            assertThat(redis.exists(lostKey)).isZero();
            assertThatThrownBy(() -> session.publishFlexStaging(lost)).hasMessageContaining("Missing or incomplete Flex staging");
            assertThat(query("SELECT count(*) FROM fw_stage")).matches("VALUES BIGINT '2'");

            var wrongType = new RediSearchPageSink(session, table, columns);
            wrongType.appendPage(page).join();
            var blocked = List.copyOf(wrongType.finish().join());
            String blockedKey = RediSearchFlexWriteBuffer.ROW_CODEC.fromJson(blocked.get(0).toStringUtf8()).get(0);
            String destination = RediSearchFlexWriteBuffer.ROW_CODEC.fromJson(redis.lindex(blockedKey, 0)).get(0);
            redis.set(destination, "wrong-type");
            try {
                assertThatThrownBy(() -> session.publishFlexStaging(blocked)).hasMessageContaining("WRONGTYPE");
            } finally {
                wrongType.abort();
                redis.del(destination);
            }
        } finally {
            session.shutdown();
            assertUpdate("DROP TABLE fw_stage");
        }
    }
}
