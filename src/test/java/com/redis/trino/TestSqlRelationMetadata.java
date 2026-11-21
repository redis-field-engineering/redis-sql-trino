package com.redis.trino;

import org.junit.jupiter.api.Test;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlRelationMetadata extends AbstractTestQueryFramework {

    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.NON_SHARDED;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        return RediSearchQueryRunner.createRediSearchQueryRunner(server);
    }

    @Test
    void testTruncatePreservesIndexSchemaAndUnrelatedRows() {
        assertUpdate("CREATE TABLE truncate_source (id bigint COMMENT 'identifier') COMMENT 'kept table'");
        assertUpdate("CREATE TABLE truncate_other (id bigint)");
        try {
            assertUpdate("INSERT INTO truncate_source SELECT x FROM UNNEST(sequence(1, 2010)) t(x)", 2010);
            assertUpdate("INSERT INTO truncate_other VALUES 99", 1);
            assertUpdate("TRUNCATE TABLE truncate_source");
            assertThat(query("SELECT count(*) FROM truncate_source")).matches("VALUES BIGINT '0'");
            assertThat(query("SELECT * FROM truncate_other")).matches("VALUES BIGINT '99'");
            assertThat(computeActual("SHOW CREATE TABLE truncate_source").getOnlyValue().toString()).contains("identifier", "kept table", "bigint");
            assertUpdate("INSERT INTO truncate_source VALUES 7", 1);
            assertThat(query("SELECT * FROM truncate_source")).matches("VALUES BIGINT '7'");
            assertUpdate("TRUNCATE TABLE truncate_source");
            assertUpdate("TRUNCATE TABLE truncate_source");
        }
        finally {
            assertUpdate("DROP TABLE truncate_source");
            assertUpdate("DROP TABLE truncate_other");
        }
    }

    @Test
    void testAddColumnPositions() {
        assertUpdate("CREATE TABLE positioned (b bigint)");
        try {
            assertUpdate("INSERT INTO positioned VALUES 2", 1);
            assertUpdate("ALTER TABLE positioned ADD COLUMN a bigint FIRST");
            assertUpdate("ALTER TABLE positioned ADD COLUMN d bigint AFTER b");
            assertUpdate("ALTER TABLE positioned ADD COLUMN c bigint AFTER b");
            assertThat(computeActual("SHOW COLUMNS FROM positioned").getMaterializedRows().stream()
                    .map(row -> row.getField(0)).toList()).containsExactly("a", "b", "c", "d");
            assertUpdate("INSERT INTO positioned VALUES (10, 20, 30, 40)", 1);
            assertThat(query("SELECT * FROM positioned")).matches("VALUES (CAST(NULL AS bigint), BIGINT '2', CAST(NULL AS bigint), CAST(NULL AS bigint)), (10, 20, 30, 40)");
        }
        finally {
            assertUpdate("DROP TABLE positioned");
        }
    }

    @Test
    void testTruncateJsonIndex() {
        var redis = server.getConnection().sync();
        redis.ftCreate("json_truncate", io.lettuce.core.search.arguments.CreateArgs.builder()
                .on(io.lettuce.core.search.arguments.CreateArgs.TargetType.JSON).withPrefix("json_truncate:").build(),
                java.util.List.of(io.lettuce.core.search.arguments.TagFieldArgs.builder().name("$.id").as("id").build()));
        server.awaitIndexed("json_truncate");
        try {
            redis.jsonSet("json_truncate:one", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":\"one\"}");
            redis.jsonSet("json_truncate:two", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":\"two\"}");
            assertThat(query("SELECT count(*) FROM json_truncate")).matches("VALUES BIGINT '2'");
            assertUpdate("TRUNCATE TABLE json_truncate");
            assertThat(query("SELECT count(*) FROM json_truncate")).matches("VALUES BIGINT '0'");
            redis.jsonSet("json_truncate:three", io.lettuce.core.json.JsonPath.ROOT_PATH, "{\"id\":\"three\"}");
            assertThat(query("SELECT id FROM json_truncate")).matches("VALUES CAST('three' AS varchar)");
        }
        finally {
            assertUpdate("DROP TABLE json_truncate");
        }
    }

    @Test
    void testViewsAndComments() {
        assertUpdate("CREATE TABLE relation_source (id bigint COMMENT 'identifier') COMMENT 'source table'");
        try {
            assertUpdate("INSERT INTO relation_source VALUES 7", 1);
            assertUpdate("CREATE VIEW relation_view AS SELECT id FROM relation_source");
            try {
                assertThat(query("SELECT * FROM relation_view")).matches("VALUES BIGINT '7'");
                assertThat(query("SELECT table_name FROM information_schema.views WHERE table_name = 'relation_view'")).matches("VALUES CAST('relation_view' AS varchar)");
                assertUpdate("CREATE OR REPLACE VIEW relation_view AS SELECT id + 1 AS id FROM relation_source");
                assertThat(query("SELECT * FROM relation_view")).matches("VALUES BIGINT '8'");
                assertUpdate("ALTER VIEW relation_view RENAME TO relation_renamed");
                assertThat(query("SELECT * FROM relation_renamed")).matches("VALUES BIGINT '8'");
                assertUpdate("ALTER VIEW relation_renamed RENAME TO relation_view");
                assertUpdate("COMMENT ON VIEW relation_view IS 'saved view'");
                assertUpdate("COMMENT ON COLUMN relation_view.id IS 'view id'");
                assertThat(computeActual("SHOW CREATE VIEW relation_view").getOnlyValue().toString()).contains("saved view");
                assertUpdate("COMMENT ON TABLE relation_source IS 'updated source'");
                assertUpdate("COMMENT ON COLUMN relation_source.id IS 'updated id'");
                assertThat(computeActual("SHOW CREATE TABLE relation_source").getOnlyValue().toString()).contains("updated source", "updated id");
                assertUpdate("COMMENT ON COLUMN relation_source.id IS NULL");
                assertUpdate("COMMENT ON TABLE relation_source IS NULL");
                assertThat(computeActual("SHOW CREATE TABLE relation_source").getOnlyValue().toString()).doesNotContain("updated source", "updated id");
            }
            finally {
                assertUpdate("DROP VIEW relation_view");
            }
        }
        finally {
            assertUpdate("DROP TABLE relation_source");
        }
    }
}
