package com.redis.trino;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** Canonicalization must preserve decoded SQL equality and reduce documents before transfer. */
public class TestCanonicalGrouping extends AbstractTestQueryFramework {
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() {
        return RedisEnterprise.Deployment.NON_SHARDED;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        var runner = RediSearchQueryRunner.createRediSearchQueryRunner(server);
        Map<String, String> fallback = new HashMap<>(server.getConnectorProperties());
        fallback.put("redisearch.default-schema-name", "tpch");
        fallback.put("redisearch.aggregation-pushdown.enabled", "false");
        runner.createCatalog("no_group_pushdown", "redisearch", fallback);
        return runner;
    }

    @Test
    void testBooleanGroupsAreCanonicalBeforeTransfer() {
        assertUpdate("CREATE TABLE normalized_bool (id varchar, flag boolean, amount double, __trino_group0 varchar)");
        try {
            String[] values = {"true", "TRUE", "TrUe", "false", "junk", "", null};
            List<RedisFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < 1050; i++) {
                Map<String, String> row = new HashMap<>(Map.of("id", Integer.toString(i), "amount", "1.5", "__trino_group0", "reserved"));
                if (values[i % values.length] != null) {
                    row.put("flag", values[i % values.length]);
                }
                writes.add(server.getConnection().async().hset("normalized_bool:" + i, row));
            }
            assertThat(LettuceFutures.awaitAll(Duration.ofSeconds(30), writes.toArray(new RedisFuture<?>[0]))).isTrue();
            assertThat(query("SELECT flag, count(*), sum(amount), count_if(flag) FROM normalized_bool GROUP BY flag"))
                    .isFullyPushedDown().matches("VALUES (true, BIGINT '450', DOUBLE '675', BIGINT '450'), "
                            + "(false, BIGINT '450', DOUBLE '675', BIGINT '0'), (CAST(NULL AS boolean), BIGINT '150', DOUBLE '225', BIGINT '0')");
            assertThat(query("SELECT flag, __trino_group0, count(*) FROM normalized_bool GROUP BY flag, __trino_group0"))
                    .isFullyPushedDown().matches("VALUES (true, VARCHAR 'reserved', BIGINT '450'), "
                            + "(false, VARCHAR 'reserved', BIGINT '450'), (CAST(NULL AS boolean), VARCHAR 'reserved', BIGINT '150')");
            QueryRunner.MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(),
                    "SELECT flag, count(*) FROM normalized_bool GROUP BY flag");
            assertThat(result.result().getRowCount()).isEqualTo(3);
            Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                    .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                    .reduce(Metrics.EMPTY, Metrics::mergeWith);
            assertThat(((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal()).isEqualTo(3);
            assertThat(query("SELECT count(DISTINCT flag) FROM normalized_bool")).matches("VALUES BIGINT '2'");
        }
        finally {
            assertUpdate("DROP TABLE normalized_bool");
        }
    }

    @Test
    void testCharacterGroupsMatchTrinoAndReduceTransfer() {
        assertUpdate("CREATE TABLE normalized_char (id varchar, c char(2), amount double, __trino_group0_offset0 varchar)");
        try {
            String[] values = {"a", "a ", "a  ", "😀", "😀 ", "😀x", "😀xy", "é中", "é中extra", "", "  ", null};
            List<RedisFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < 1200; i++) {
                Map<String, String> row = new HashMap<>(Map.of("id", Integer.toString(i), "amount", "2", "__trino_group0_offset0", "reserved"));
                if (values[i % values.length] != null) {
                    row.put("c", values[i % values.length]);
                }
                writes.add(server.getConnection().async().hset("normalized_char:" + i, row));
            }
            assertThat(LettuceFutures.awaitAll(Duration.ofSeconds(30), writes.toArray(new RedisFuture<?>[0]))).isTrue();
            String suffix = " GROUP BY c";
            assertThat(query("SELECT c, count(*), sum(amount) FROM normalized_char" + suffix)).isFullyPushedDown()
                    .matches("SELECT c, count(*), sum(amount) FROM no_group_pushdown.tpch.normalized_char" + suffix);
            assertThat(query("SELECT count(DISTINCT c) FROM normalized_char"))
                    .matches("SELECT count(DISTINCT c) FROM no_group_pushdown.tpch.normalized_char");
            String grouped = "SELECT c, count(*) FROM normalized_char" + suffix;
            long pushedStart = System.nanoTime();
            var pushed = getDistributedQueryRunner().executeWithPlan(getSession(), grouped);
            double pushedMillis = (System.nanoTime() - pushedStart) / 1_000_000.0;
            long fallbackStart = System.nanoTime();
            var fallback = getDistributedQueryRunner().executeWithPlan(getSession(),
                    "SELECT c, count(*) FROM no_group_pushdown.tpch.normalized_char" + suffix);
            double fallbackMillis = (System.nanoTime() - fallbackStart) / 1_000_000.0;
            assertThat(sourceRows(pushed)).isEqualTo(6);
            assertThat(sourceRows(fallback)).isEqualTo(1200);
            System.out.printf("CANONICAL_CHAR pushedRows=%s fallbackRows=%s pushedWallMs=%.3f fallbackWallMs=%.3f pushedCpuMs=%s fallbackCpuMs=%s%n",
                    sourceRows(pushed), sourceRows(fallback), pushedMillis, fallbackMillis, queryCpuMillis(pushed), queryCpuMillis(fallback));
        }
        finally {
            assertUpdate("DROP TABLE normalized_char");
        }
    }

    private long sourceRows(QueryRunner.MaterializedResultWithPlan result) {
        Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
        return ((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal();
    }

    private double queryCpuMillis(QueryRunner.MaterializedResultWithPlan result) {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getTotalCpuTime().getValue(java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Test
    void testBooleanWhitespaceAndControlsMatchSqlDecoder() {
        assertUpdate("CREATE TABLE normalized_bool_controls (id varchar, flag boolean)");
        try {
            String[] values = {"true", "TRUE", "TrUe", "tRuE", "false", "FALSE", "junk", "", " true", "true ",
                    "true" + (char) 0 + "tail", (char) 0 + "true", "ＴＲＵＥ", "trué", null, null};
            for (int i = 0; i < values.length; i++) {
                Map<String, String> row = new HashMap<>(Map.of("id", Integer.toString(i)));
                if (values[i] != null) {
                    row.put("flag", values[i]);
                }
                server.getConnection().sync().hset("normalized_bool_controls:" + i, row);
            }
            assertThat(query("SELECT flag, count(*) FROM normalized_bool_controls GROUP BY flag"))
                    .isFullyPushedDown().matches("VALUES (true, BIGINT '4'), (false, BIGINT '10'), (CAST(NULL AS boolean), BIGINT '2')");
        }
        finally {
            assertUpdate("DROP TABLE normalized_bool_controls");
        }
    }

    @Test
    void testCharacterBoundsAndAllNullInputs() {
        assertUpdate("CREATE TABLE normalized_char_bounds (id varchar, b boolean, c0 char(0), c8 char(8), c9 char(9))");
        try {
            var redis = server.getConnection().sync();
            redis.hset("normalized_char_bounds:1", Map.of("id", "1", "c0", "discard", "c8", "😀é中xy z trailing", "c9", "small"));
            redis.hset("normalized_char_bounds:2", Map.of("id", "2", "c0", "", "c8", "😀é中xy z ", "c9", "small "));
            redis.hset("normalized_char_bounds:3", Map.of("id", "3"));
            assertThat(query("SELECT c0, count(*) FROM normalized_char_bounds GROUP BY c0")).isFullyPushedDown()
                    .matches("VALUES (CAST('' AS char(0)), BIGINT '2'), (CAST(NULL AS char(0)), BIGINT '1')");
            assertThat(query("SELECT c8, count(*) FROM normalized_char_bounds GROUP BY c8")).isFullyPushedDown()
                    .matches("SELECT c8, count(*) FROM no_group_pushdown.tpch.normalized_char_bounds GROUP BY c8");
            assertThat(query("SELECT c9, count(*) FROM normalized_char_bounds GROUP BY c9"))
                    .isNotFullyPushedDown(io.trino.sql.planner.plan.AggregationNode.class)
                    .matches("SELECT c9, count(*) FROM no_group_pushdown.tpch.normalized_char_bounds GROUP BY c9");
            assertThat(query("SELECT b, count(*) FROM normalized_char_bounds GROUP BY b")).isFullyPushedDown()
                    .matches("VALUES (CAST(NULL AS boolean), BIGINT '3')");
        }
        finally {
            assertUpdate("DROP TABLE normalized_char_bounds");
        }
    }

    @Test
    void testDistinctStageKeepsCanonicalNullGroupsSeparate() {
        assertUpdate("CREATE TABLE normalized_stages (id varchar, b boolean, c char(2), d double)");
        try {
            List<Map<String, String>> rows = List.of(
                    Map.of("id", "a", "b", "false", "c", "a", "d", "-0.0"),
                    Map.of("id", "a", "b", "garbage", "c", "a ", "d", "0.0"),
                    Map.of("id", "b", "b", "false", "c", "a", "d", "0.0"),
                    Map.of("id", "a"), Map.of("id", "a"), Map.of("id", "b"),
                    Map.of("b", "false", "c", "a", "d", "0.0"), Map.of("other", "unindexed"));
            for (int i = 0; i < rows.size(); i++) {
                server.getConnection().sync().hset("normalized_stages:" + i, rows.get(i));
            }
            for (String key : List.of("b", "c", "d")) {
                String sql = "SELECT " + key + ", count(DISTINCT id) FROM normalized_stages GROUP BY " + key;
                assertThat(query(sql)).isFullyPushedDown().matches("SELECT " + key
                        + ", count(DISTINCT id) FROM no_group_pushdown.tpch.normalized_stages GROUP BY " + key);
                var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
                assertThat(result.result().getRowCount()).isEqualTo(2);
                assertThat(sourceRows(result)).isEqualTo(2);
            }
        }
        finally {
            assertUpdate("DROP TABLE normalized_stages");
        }
    }

    @Test
    void testBooleanHashAliasUsesItsIndexedSource() {
        var redis = server.getConnection().sync();
        redis.ftCreate("normalized_alias", CreateArgs.builder().withPrefix("normalized_alias:").build(), List.of(
                TagFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("raw_flag").as("flag").build()));
        redis.set(RediSearchColumnTypes.key("normalized_alias"), "{\"id\":\"varchar\",\"flag\":\"boolean\"}");
        server.awaitIndexed("normalized_alias");
        try {
            redis.hset("normalized_alias:1", Map.of("id", "1", "raw_flag", "TRUE"));
            redis.hset("normalized_alias:2", Map.of("id", "2", "raw_flag", "true"));
            redis.hset("normalized_alias:3", Map.of("id", "3", "raw_flag", "other"));
            redis.hset("normalized_alias:4", Map.of("id", "4"));
            assertThat(query("SELECT flag, count(*) FROM normalized_alias GROUP BY flag"))
                    .isFullyPushedDown().matches("VALUES (true, BIGINT '2'), (false, BIGINT '1'), (CAST(NULL AS boolean), BIGINT '1')");
        }
        finally {
            assertUpdate("DROP TABLE normalized_alias");
        }
    }
}
