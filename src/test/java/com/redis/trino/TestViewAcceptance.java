package com.redis.trino;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import io.airlift.json.JsonCodec;
import io.trino.Session;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TypeId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingAccessControlManager.TestingPrivilege;

import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Ordinary-view acceptance through Trino, including policy checks under the stored security identity. */
@Execution(ExecutionMode.SAME_THREAD)
public class TestViewAcceptance extends AbstractTestQueryFramework {
    private RediSearchServer server;

    protected RedisEnterprise.Deployment deployment() { return RedisEnterprise.Deployment.NON_SHARDED; }

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = closeAfterClass(new RediSearchServer(deployment()));
        return RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                Map.of("redisearch.table-cache-refresh", "0"));
    }

    @Test
    void testPersistedViewTypesAndResolutionIgnoreTheReaderSessionContext() {
        Session owner = user("view_owner");
        Session elsewhere = Session.builder(user("view_reader")).setCatalog("tpch").setSchema("tiny").build();
        assertUpdate("CREATE TABLE view_context_source (id bigint, label varchar)");
        try {
            assertUpdate("INSERT INTO view_context_source VALUES (7, 'kept')", 1);
            assertUpdate(owner, "CREATE VIEW view_context SECURITY INVOKER AS "
                    + "SELECT CAST(id AS decimal(20,2)) AS amount, label FROM view_context_source");
            try {
                ConnectorViewDefinition saved = storedView("view_context");
                assertThat(saved.getCatalog()).contains("redisearch");
                assertThat(saved.getSchema()).contains("tpch");
                assertThat(saved.isRunAsInvoker()).isTrue();
                assertThat(saved.getColumns().stream().map(ConnectorViewDefinition.ViewColumn::getName).toList())
                        .containsExactly("amount", "label");
                assertThat(saved.getColumns().stream().map(ConnectorViewDefinition.ViewColumn::getType).toList())
                        .containsExactly(TypeId.of("decimal(20,2)"), TypeId.of("varchar"));
                assertThat(query(elsewhere, "SELECT * FROM redisearch.tpch.view_context"))
                        .matches("VALUES (CAST(7 AS decimal(20,2)), VARCHAR 'kept')");
                assertThat(computeActual(elsewhere, "SHOW CREATE VIEW redisearch.tpch.view_context").getOnlyValue().toString())
                        .contains("SECURITY INVOKER", "view_context_source");
                assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = 'tpch' AND table_name = 'view_context'"))
                        .matches("VALUES VARCHAR 'view_context'");
                assertThat(query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch' "
                        + "AND table_name = 'view_context' AND table_type = 'VIEW'"))
                        .matches("VALUES VARCHAR 'view_context'");
                assertThat(query("SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_schema = 'tpch' AND table_name = 'view_context'"))
                        .matches("VALUES (VARCHAR 'amount', VARCHAR 'decimal(20,2)'), (VARCHAR 'label', VARCHAR 'varchar')");
            }
            finally {
                assertUpdate("DROP VIEW view_context");
            }
            assertThat(storedViews()).doesNotContainKey("view_context");
            assertQueryReturnsEmptyResult("SELECT table_name FROM information_schema.views WHERE table_name = 'view_context'");
            assertQueryFails("SELECT * FROM view_context", ".*does not exist.*");
        }
        finally {
            assertUpdate("DROP TABLE view_context_source");
        }
    }

    @Test
    void testDefinerAndInvokerUseDifferentSourcePrivileges() {
        Session owner = user("view_owner");
        Session reader = user("view_reader");
        assertUpdate("CREATE TABLE view_security_source (id bigint)");
        try {
            assertUpdate("INSERT INTO view_security_source VALUES 7", 1);
            assertUpdate(owner, "CREATE VIEW view_security_definer SECURITY DEFINER AS "
                    + "SELECT id, current_user AS querying_user FROM view_security_source");
            assertUpdate(owner, "CREATE VIEW view_security_invoker SECURITY INVOKER AS "
                    + "SELECT id, current_user AS querying_user FROM view_security_source");
            try {
                assertThat(storedView("view_security_definer").getOwner()).contains("view_owner");
                assertThat(storedView("view_security_definer").isRunAsInvoker()).isFalse();
                assertThat(storedView("view_security_invoker").isRunAsInvoker()).isTrue();
                // Trino's CURRENT_USER function returns the query caller in both view modes.
                // The separate source-privilege denials below verify the stored authorization identity.
                assertThat(query(reader, "SELECT * FROM view_security_definer"))
                        .matches("VALUES (BIGINT '7', VARCHAR 'view_reader')");
                assertThat(query(reader, "SELECT * FROM view_security_invoker"))
                        .matches("VALUES (BIGINT '7', VARCHAR 'view_reader')");

                var access = getQueryRunner().getAccessControl();
                access.deny(privilege("view_reader", "view_security_source", SELECT_COLUMN));
                try {
                    assertThatThrownBy(() -> computeActual(reader, "SELECT id FROM view_security_source"))
                            .hasMessageContaining("Access Denied").hasMessageContaining("view_security_source");
                    assertThat(query(reader, "SELECT * FROM view_security_definer"))
                            .matches("VALUES (BIGINT '7', VARCHAR 'view_reader')");
                    assertThatThrownBy(() -> computeActual(reader, "SELECT * FROM view_security_invoker"))
                            .hasMessageContaining("Access Denied").hasMessageContaining("view_security_source");
                }
                finally {
                    access.reset();
                }

                assertDenied(reader, "SELECT * FROM view_security_definer", "view_security_source",
                        privilege("view_owner", "view_security_source", CREATE_VIEW_WITH_SELECT_COLUMNS));
                assertDenied(reader, "SELECT * FROM view_security_definer", "view_security_definer",
                        privilege("view_reader", "view_security_definer", SELECT_COLUMN));
                assertDenied(owner, "DROP VIEW view_security_definer", "view_security_definer",
                        privilege("view_owner", "view_security_definer", DROP_VIEW));
                assertThat(query(reader, "SELECT id FROM view_security_definer")).matches("VALUES BIGINT '7'");

                assertDenied(owner, "CREATE VIEW view_security_denied AS SELECT id FROM view_security_source", "view_security_denied",
                        privilege("view_owner", "view_security_denied", CREATE_VIEW));
                assertThat(storedViews()).doesNotContainKey("view_security_denied");
            }
            finally {
                getQueryRunner().getAccessControl().reset();
                assertUpdate("DROP VIEW IF EXISTS view_security_definer");
                assertUpdate("DROP VIEW IF EXISTS view_security_invoker");
                assertUpdate("DROP VIEW IF EXISTS view_security_denied");
            }
        }
        finally {
            assertUpdate("DROP TABLE view_security_source");
        }
    }

    @Test
    void testReferencedTableEvolutionIsReanalyzedWithoutRewritingTheSavedView() {
        assertUpdate("CREATE TABLE view_evolution_source (id bigint)");
        try {
            assertUpdate("INSERT INTO view_evolution_source VALUES 7", 1);
            assertUpdate("CREATE VIEW view_evolution AS SELECT id FROM view_evolution_source");
            try {
                String originalDefinition = storedViews().get("view_evolution");
                assertUpdate("ALTER TABLE view_evolution_source ADD COLUMN extra varchar");
                assertThat(query("SELECT * FROM view_evolution")).matches("VALUES BIGINT '7'");
                assertUpdate("DROP TABLE view_evolution_source");
                assertQueryFails("SELECT * FROM view_evolution", ".*does not exist.*");
                assertThat(storedViews().get("view_evolution")).isEqualTo(originalDefinition);

                assertUpdate("CREATE TABLE view_evolution_source (id bigint)");
                assertUpdate("INSERT INTO view_evolution_source VALUES 9", 1);
                assertThat(query("SELECT * FROM view_evolution")).matches("VALUES BIGINT '9'");
                assertUpdate("DROP TABLE view_evolution_source");
                assertUpdate("CREATE TABLE view_evolution_source (id varchar)");
                assertUpdate("INSERT INTO view_evolution_source VALUES 'changed'", 1);
                assertQueryFails("SELECT * FROM view_evolution", ".*stale.*");
                assertThat(storedViews().get("view_evolution")).isEqualTo(originalDefinition);
                assertUpdate("CREATE OR REPLACE VIEW view_evolution AS SELECT id FROM view_evolution_source");
                assertThat(query("SELECT * FROM view_evolution")).matches("VALUES VARCHAR 'changed'");
                assertThat(storedView("view_evolution").getColumns().getFirst().getType()).isEqualTo(TypeId.of("varchar"));
            }
            finally {
                assertUpdate("DROP VIEW view_evolution");
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS view_evolution_source");
        }
    }

    private void assertDenied(Session session, String sql, String object, TestingPrivilege... privileges) {
        var access = getQueryRunner().getAccessControl();
        access.deny(privileges);
        try {
            assertThatThrownBy(() -> computeActual(session, sql)).hasMessageContaining("Access Denied").hasMessageContaining(object);
        }
        finally {
            access.reset();
        }
    }

    private Session user(String user) {
        return Session.builder(getSession()).setIdentity(Identity.ofUser(user)).build();
    }

    private Map<String, String> storedViews() {
        String json = server.getConnection().sync().get(RediSearchRelationMetadata.viewsKey("tpch"));
        return json == null ? Map.of() : JsonCodec.mapJsonCodec(String.class, String.class).fromJson(json);
    }

    private ConnectorViewDefinition storedView(String view) {
        return JsonCodec.jsonCodec(ConnectorViewDefinition.class).fromJson(storedViews().get(view));
    }
}
