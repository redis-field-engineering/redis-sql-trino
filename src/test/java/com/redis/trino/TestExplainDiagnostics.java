package com.redis.trino;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExplainDiagnostics extends AbstractTestQueryFramework {
    private RediSearchServer server;

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer());
        var redis = server.getConnection().sync();
        redis.ftCreate("explain_diag", CreateArgs.builder().withPrefix("explain_diag:").build(), List.of(
                NumericFieldArgs.builder().name("id").build(), TagFieldArgs.builder().name("label").build()));
        for (int i = 0; i < 3; i++) { redis.hset("explain_diag:" + i, Map.of("id", Integer.toString(i), "label", "a")); }
        server.awaitIndexed("explain_diag");
        var runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.aggregation-group-limit", "2"));
        try {
            Map<String, String> disabled = new HashMap<>(server.getConnectorProperties());
            disabled.put("redisearch.default-schema-name", "tpch");
            disabled.put("redisearch.aggregation-pushdown.enabled", "false");
            disabled.put("redisearch.aggregation-group-limit", "1");
            disabled.put("redisearch.local-topn.max-candidates", "0");
            disabled.put("redisearch.resp2", "true");
            runner.createCatalog("diag_disabled", "redisearch", disabled);
            return runner;
        } catch (RuntimeException | Error failure) {
            RediSearchQueryRunner.closeAllSuppress(failure, runner);
            throw failure;
        }
    }

    @Test
    void testExplainShowsPolicyAndAppliedStagesWithoutChangingResults() {
        String enabled = explain("SELECT count(*) FROM explain_diag");
        assertThat(enabled).contains("pushdownPolicy", "planning_policy_not_rejection_history", "cached_documents=3",
                "aggregation=enabled_subject_to_expression_safety", "appliedPushdowns", "aggregation_stages=1");
        String disabled = explain("SELECT count(*) FROM diag_disabled.tpch.explain_diag");
        assertThat(disabled).contains("aggregation=disabled_by_configuration", "floating_aggregates=blocked_by_resp2_precision",
                "group_budget=1", "finite_type_or_exact_source_domain_proof_required", "local_topn=disabled_by_configuration",
                "aggregation_stages=0");
        assertThat(query("SELECT count(*) FROM explain_diag")).matches("VALUES BIGINT '3'");
        assertThat(explain("SELECT id FROM explain_diag ORDER BY id LIMIT 2")).contains("ordering=redis_numeric_sort");
        assertThat(explain("SELECT label FROM explain_diag ORDER BY label LIMIT 2")).contains("ordering=connector_typed_topn");
    }

    @Test
    void testNativeFunctionAlsoHasImmutablePolicy() {
        assertThat(explain("SELECT id FROM TABLE(redisearch.system.search(TABLE_NAME=>'explain_diag', QUERY=>'@label:$term', "
                + "PARAMETERS=>MAP(ARRAY['term'],ARRAY[to_utf8('a')])))"))
                .contains("pushdownPolicy", "global_parameterized_query", "aggregation=enabled_subject_to_expression_safety");
    }

    @Test
    void testConnectorInfoExposesSameImmutableSnapshot() {
        getQueryRunner().inTransaction(session -> {
            var metadata = getQueryRunner().getPlannerContext().getMetadata();
            var handle = metadata.getTableHandle(session, new QualifiedObjectName("redisearch", "tpch", "explain_diag")).orElseThrow();
            var connector = (RediSearchTableHandle) handle.connectorHandle();
            assertThat(metadata.getInfo(session, handle).orElseThrow()).isEqualTo(Map.of(
                    "planningPolicy", connector.getPushdownPolicy(), "appliedPushdowns", RediSearchPlanDiagnostics.applied(connector)));
            return null;
        });
    }

    @Test
    void testDisabledCacheDoesNotDescribeTwiceForDiagnostics() {
        var config = new RediSearchConfig().setUri(server.getRedisURI()).setDefaultSchema("tpch")
                .setTableCacheRefresh(0);
        var session = new RediSearchSession(getQueryRunner().getPlannerContext().getTypeManager(), config) {
            private int describes;

            @Override
            public RediSearchTable getTable(SchemaTableName name) {
                describes++;
                return super.getTable(name);
            }
        };
        try {
            var metadata = new RediSearchMetadata(session);
            var handle = metadata.getTableHandle(null, new SchemaTableName("tpch", "explain_diag"), Optional.empty(), Optional.empty());
            assertThat(handle.getPushdownPolicy()).containsEntry("cached_documents", "3");
            assertThat(session.describes).isEqualTo(1);
            assertThat(session.getCachedCaseSupport()).isEmpty();
            metadata.getInfo(null, handle);
            assertThat(session.describes).isEqualTo(1);
        } finally { session.shutdown(); }
    }

    private String explain(String sql) { return (String) computeActual("EXPLAIN " + sql).getOnlyValue(); }
}
