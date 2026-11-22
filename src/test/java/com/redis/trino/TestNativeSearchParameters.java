package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.GeoFieldArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.lettuce.core.search.arguments.TextFieldArgs;
import io.lettuce.core.search.arguments.VectorFieldArgs;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

public class TestNativeSearchParameters extends AbstractTestQueryFramework {
    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        RediSearchServer server = closeAfterClass(new RediSearchServer(deployment()));
        var redis = server.getConnection().sync();
        redis.ftCreate("native_vectors", CreateArgs.builder().withPrefix("native_vector:").build(), List.of(
                NumericFieldArgs.builder().name("id").build(), NumericFieldArgs.builder().name("rank").build(),
                TagFieldArgs.builder().name("category").build(), TextFieldArgs.builder().name("name").build(),
                GeoFieldArgs.builder().name("location").build(), VectorFieldArgs.builder().name("vec").flat()
                        .type(VectorFieldArgs.VectorType.FLOAT32).dimensions(2).distanceMetric(VectorFieldArgs.DistanceMetric.L2).build()));
        server.awaitIndexed("native_vectors");
        List<String> keys = server.keysOnShards("native_vector:", 0, 1, 0, 1);
        double[] ranks = {50, 10, 100, 70};
        String[] locations = {"-122.4194,37.7749", "-122.42,37.775", "-74.006,40.7128", "-118.24,34.05"};
        for (int i = 0; i < keys.size(); i++) {
            redis.hset(keys.get(i), Map.of("id", Integer.toString(i + 1), "rank", Double.toString(ranks[i]),
                    "category", i == 0 ? "blocked" : "keep", "name", i == 2 ? "Other engine" : "Redis search", "location", locations[i]));
            byte[] vector = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putFloat(i).putFloat(0).array();
            redis.dispatch(CommandType.HSET, new IntegerOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8)
                    .addKey(keys.get(i)).add("vec").add(vector));
        }
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(), Map.of(
                "redisearch.cursor-count", "1", "redisearch.scan-splits", "4", "redisearch.scan-partition-field", "id",
                "redisearch.scan-partition-boundaries", "1.5,2.5,3.5", "redisearch.dynamic-filtering.wait-timeout", "1s"));
    }

    private static String source(String query, String parameters) {
        return "TABLE(redisearch.system.search(TABLE_NAME => 'native_vectors', QUERY => '" + query
                + "', PARAMETERS => " + parameters + "))";
    }

    private static String nearest() {
        return source("*=>[KNN 2 @vec $blob]", "MAP(ARRAY['blob'], ARRAY[X'0000000000000000'])");
    }

    @Test
    void testTextNumericAndGeoBinaryParameters() {
        assertThat(query("SELECT id FROM " + source("@name:$term", "MAP(ARRAY['term'], ARRAY[to_utf8('redis')])")))
                .isFullyPushedDown().matches("VALUES DOUBLE '1', DOUBLE '2', DOUBLE '4'");
        assertThat(query("SELECT id FROM " + source("@rank:[$lo $hi]", "MAP(ARRAY['lo','hi'], ARRAY[to_utf8('9'),to_utf8('51')])")))
                .isFullyPushedDown().matches("VALUES DOUBLE '1', DOUBLE '2'");
        assertThat(query("SELECT id FROM " + source("@location:[$lon $lat $radius km]",
                "MAP(ARRAY['lon','lat','radius'], ARRAY[to_utf8('-122.4194'),to_utf8('37.7749'),to_utf8('1')])")))
                .isFullyPushedDown().matches("VALUES DOUBLE '1', DOUBLE '2'");
    }

    @Test
    void testGlobalKnnMembershipAndBinaryFloatPayload() {
        String sql = "SELECT id FROM " + nearest();
        assertThat(query(sql)).isFullyPushedDown().matches("VALUES DOUBLE '1', DOUBLE '2'");
        var result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        Metrics metrics = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                .reduce(Metrics.EMPTY, Metrics::mergeWith);
        assertThat(((LongCount) metrics.getMetrics().get("redis.aggregate.requests")).getTotal()).isEqualTo(1);
        assertThat(((LongCount) metrics.getMetrics().get("redis.rows.received")).getTotal()).isEqualTo(2);
        // FLOAT32 1.125 contains invalid UTF-8 0x90 in addition to embedded NULs: never pass through StringCodec.
        assertThat(query("SELECT id FROM " + source("*=>[KNN 2 @vec $blob]", "MAP(ARRAY['blob'], ARRAY[X'0000903F00000000'])")))
                .isFullyPushedDown().matches("VALUES DOUBLE '2', DOUBLE '3'");
        assertThat(query("SELECT count(*) FROM " + nearest())).isFullyPushedDown().matches("VALUES BIGINT '2'");
        // A native hybrid condition intentionally precedes KNN, unlike an outer SQL WHERE.
        assertThat(query("SELECT id FROM " + source("@category:{$tag}=>[KNN 2 @vec $blob]",
                "MAP(ARRAY['tag','blob'], ARRAY[to_utf8('keep'),X'0000000000000000'])")))
                .isFullyPushedDown().matches("VALUES DOUBLE '2', DOUBLE '3'");
    }

    @Test
    void testSqlFiltersAndDynamicFiltersRunAfterGlobalKnn() {
        assertThat(query("SELECT id FROM " + nearest() + " WHERE category = 'keep'"))
                .isNotFullyPushedDown(FilterNode.class).matches("VALUES DOUBLE '2'");
        assertThat(query("SELECT id FROM " + nearest() + " WHERE id > 1"))
                .isNotFullyPushedDown(FilterNode.class).matches("VALUES DOUBLE '2'");
        assertThat(query("SELECT count(*) FROM " + nearest() + " WHERE category = 'keep'"))
                .matches("VALUES BIGINT '1'");
        assertUpdate("CREATE TABLE native_join_keys (id double)");
        try {
            assertUpdate("INSERT INTO native_join_keys VALUES 2.0, 3.0", 2);
            String join = "SELECT n.id FROM " + nearest() + " n JOIN native_join_keys k ON n.id = k.id";
            Session enabled = Session.builder(getSession()).setSystemProperty("join_reordering_strategy", "NONE")
                    .setSystemProperty("join_distribution_type", "BROADCAST").setSystemProperty("enable_dynamic_filtering", "true").build();
            Session disabled = Session.builder(enabled).setSystemProperty("enable_dynamic_filtering", "false").build();
            assertThat(query(enabled, join)).matches("VALUES DOUBLE '2'");
            assertThat(query(disabled, join)).matches("VALUES DOUBLE '2'");
        } finally {
            assertUpdate("DROP TABLE native_join_keys");
        }
    }

    @Test
    void testSqlSortAndLimitStayWithinKnnRowset() {
        assertThat(query("SELECT id FROM " + nearest() + " ORDER BY rank DESC LIMIT 1"))
                .isNotFullyPushedDown(TopNNode.class).matches("VALUES DOUBLE '1'");
        assertThat(query("SELECT id FROM " + nearest() + " ORDER BY category DESC LIMIT 1"))
                .isNotFullyPushedDown(TopNNode.class).matches("VALUES DOUBLE '2'");
        assertThat(query("SELECT count(*) FROM (SELECT id FROM " + nearest() + " LIMIT 1)"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    void testInvalidParameterMaps() {
        assertQueryFails("SELECT id FROM " + source("*", "CAST(NULL AS MAP(VARCHAR,VARBINARY))"), ".*PARAMETERS must be a non-null.*");
        assertQueryFails("SELECT id FROM " + source("*", "MAP(ARRAY['x'], ARRAY[CAST(NULL AS VARBINARY)])"), ".*keys and values must not be null.*");
        assertQueryFails("SELECT id FROM " + source("*", "MAP(ARRAY[''], ARRAY[X'00'])"), ".*keys must be non-empty.*");
    }
}
