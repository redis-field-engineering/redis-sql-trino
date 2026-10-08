package com.redis.trino;

import java.util.Map;

/** Existing exact reads, warning rejection, cancellation and cleanup coverage with four partitions. */
public class TestParallelCursorReads extends TestCursorReads {
    @Override
    protected Map<String, String> scanProperties() {
        return Map.of("redisearch.cursor-count", "7", "redisearch.scan-splits", "4",
                "redisearch.scan-partition-field", "id", "redisearch.scan-partition-boundaries", "0,25,50");
    }

    @Override
    protected long scanAggregateRequests() { return 4; }
    @org.junit.jupiter.api.Test
    public void testBoundariesFiltersJoinAndGlobalOperations() {
        org.assertj.core.api.Assertions.assertThat(query("SELECT id FROM many WHERE id IN (1,25,50,100)"))
                .matches("VALUES DOUBLE '1', DOUBLE '25', DOUBLE '50', DOUBLE '100'");
        assertUpdate("CREATE TABLE parallel_join_keys (id double)");
        try {
            assertUpdate("INSERT INTO parallel_join_keys VALUES 25.0, 50.0, 75.0", 3);
            String join = "SELECT count(DISTINCT m.id) FROM many m JOIN parallel_join_keys k ON m.id = k.id";
            org.assertj.core.api.Assertions.assertThat(query(join)).matches("VALUES BIGINT '3'");
            var fixedJoin = io.trino.Session.builder(getSession()).setSystemProperty("join_reordering_strategy", "NONE")
                    .setSystemProperty("join_distribution_type", "BROADCAST").build();
            var withoutFilters = io.trino.Session.builder(fixedJoin)
                    .setSystemProperty("enable_dynamic_filtering", "false").build();
            var without = getDistributedQueryRunner().executeWithPlan(withoutFilters, join);
            var with = getDistributedQueryRunner().executeWithPlan(fixedJoin, join);
            var queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
            org.assertj.core.api.Assertions.assertThat(queryManager.getFullQueryInfo(without.queryId())
                    .getQueryStats().getPhysicalInputPositions()).isEqualTo(103);
            org.assertj.core.api.Assertions.assertThat(queryManager.getFullQueryInfo(with.queryId())
                    .getQueryStats().getPhysicalInputPositions()).isEqualTo(6);
        } finally {
            assertUpdate("DROP TABLE parallel_join_keys");
        }
        org.assertj.core.api.Assertions.assertThat(query("SELECT id FROM many ORDER BY id DESC LIMIT 3"))
                .ordered().matches("VALUES DOUBLE '100', DOUBLE '99', DOUBLE '98'");
        org.assertj.core.api.Assertions.assertThat(count(connectorMetrics("SELECT id FROM many ORDER BY id DESC LIMIT 3"),
                "redis.aggregate.requests")).isEqualTo(1);
        org.assertj.core.api.Assertions.assertThat(count(connectorMetrics("SELECT id, count(*) FROM many GROUP BY id"),
                "redis.aggregate.requests")).isEqualTo(1);
    }

    @org.junit.jupiter.api.Test
    public void testMultiValuedJsonPartitionsDoNotOverlap() {
        var redis = redisearch.getConnection().sync();
        redis.ftCreate("parallel_json", io.lettuce.core.search.arguments.CreateArgs.builder()
                .on(io.lettuce.core.search.arguments.CreateArgs.TargetType.JSON).withPrefix("parallel_json:").build(),
                java.util.List.of(io.lettuce.core.search.arguments.NumericFieldArgs.builder().name("$.numbers[*]").as("id").build(),
                        io.lettuce.core.search.arguments.TagFieldArgs.builder().name("$.marker").as("marker").build()));
        redisearch.awaitIndexed("parallel_json");
        String[] documents = {"{\"numbers\":[-1,100],\"marker\":\"cross\"}",
                "{\"numbers\":[0,25],\"marker\":\"boundary\"}",
                "{\"numbers\":[26,49],\"marker\":\"middle\"}",
                "{\"numbers\":[],\"marker\":\"empty\"}", "{\"marker\":\"missing\"}"};
        for (int i = 0; i < documents.length; i++) {
            redis.jsonSet("parallel_json:" + i, io.lettuce.core.json.JsonPath.ROOT_PATH, documents[i]);
        }
        org.assertj.core.api.Assertions.assertThat(query("SELECT marker FROM parallel_json"))
                .matches("VALUES VARCHAR 'cross', VARCHAR 'boundary', VARCHAR 'middle', VARCHAR 'empty', VARCHAR 'missing'");
        var metrics = connectorMetrics("SELECT marker FROM parallel_json");
        org.assertj.core.api.Assertions.assertThat(count(metrics, "redis.rows.received")).isEqualTo(5);
        org.assertj.core.api.Assertions.assertThat(count(metrics, "redis.aggregate.requests")).isEqualTo(4);
    }
}
