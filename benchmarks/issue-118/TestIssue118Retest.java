package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.trino.testing.DistributedQueryRunner;

/** Reproduction harness: assertions verify observed failures, not issue acceptance. */
public class TestIssue118Retest {
    private static void record(String label, Object value) {
        System.out.println("ISSUE118 " + label + ": " + value);
    }

    private static Object raw(StatefulRedisConnection<String, String> connection, String command, String... args) {
        ProtocolKeyword keyword = () -> command.getBytes(StandardCharsets.US_ASCII);
        CommandArgs<String, String> arguments = new CommandArgs<>(StringCodec.UTF8);
        for (String arg : args) {
            arguments.add(arg);
        }
        return connection.sync().dispatch(keyword, new NestedMultiOutput<>(StringCodec.UTF8),
                arguments);
    }

    private static String failure(Runnable operation) {
        String result = outcome(operation);
        assertThat(result).isNotEqualTo("SUCCEEDED");
        return result;
    }

    private static String outcome(Runnable operation) {
        try {
            operation.run();
            return "SUCCEEDED";
        } catch (RuntimeException e) {
            StringBuilder result = new StringBuilder();
            for (Throwable t = e; t != null; t = t.getCause()) {
                result.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" | ");
            }
            return result.toString();
        }
    }

    private static void catalog(DistributedQueryRunner runner, String name, int port) {
        runner.createCatalog(name, "redisearch", Map.of("redisearch.uri", "redis://127.0.0.1:" + port,
                "redisearch.default-schema-name", "tpch"));
    }

    @Test
    public void retestLocalDockerTargets() throws Exception {
        RedisClient client = RedisClient.create();
        try (var ram = client.connect(io.lettuce.core.RedisURI.create("redis://127.0.0.1:12018"));
             var flex = client.connect(io.lettuce.core.RedisURI.create("redis://127.0.0.1:12019"));
             var missing = client.connect(io.lettuce.core.RedisURI.create("redis://127.0.0.1:12020"));
             var acl = client.connect(io.lettuce.core.RedisURI.create("redis://127.0.0.1:12021"));
             var runner = DistributedQueryRunner.builder(RediSearchQueryRunner.createSession()).build()) {
            for (var target : Map.of("ram", ram, "flex", flex, "missing", missing).entrySet()) {
                record(target.getKey() + " PING", target.getValue().sync().ping());
                record(target.getKey() + " MODULE LIST", raw(target.getValue(), "MODULE", "LIST"));
                record(target.getKey() + " COMMAND INFO", raw(target.getValue(), "COMMAND", "INFO", "FT._LIST", "FT.AGGREGATE"));
            }
            runner.installPlugin(new RediSearchPlugin());
            catalog(runner, "redisearch", 12018);
            catalog(runner, "flex", 12019);
            catalog(runner, "missing", 12020);
            record("catalog creation without Search", "succeeded; no capability rejection");
            runner.execute("CREATE TABLE redisearch.tpch.issue118_ram (id varchar, name varchar)");
            runner.execute("INSERT INTO redisearch.tpch.issue118_ram VALUES ('1', 'local')");
            assertThat(runner.execute("SELECT name FROM redisearch.tpch.issue118_ram WHERE id = '1'").getOnlyValue()).isEqualTo("local");
            assertThat(runner.execute("SELECT count(*) FROM redisearch.tpch.issue118_ram").getOnlyValue()).isEqualTo(1L);
            runner.execute("ALTER TABLE redisearch.tpch.issue118_ram ADD COLUMN extra varchar");
            runner.execute("UPDATE redisearch.tpch.issue118_ram SET extra = 'updated' WHERE id = '1'");
            assertThat(runner.execute("SELECT extra FROM redisearch.tpch.issue118_ram WHERE id = '1'").getOnlyValue()).isEqualTo("updated");
            runner.execute("DELETE FROM redisearch.tpch.issue118_ram WHERE id = '1'");
            assertThat(runner.execute("SELECT count(*) FROM redisearch.tpch.issue118_ram").getOnlyValue()).isEqualTo(0L);
            record("Software RAM mutations", "ADD COLUMN / UPDATE and readback / DELETE and readback passed");
            runner.execute("CREATE TABLE redisearch.tpch.issue118_numbers (id bigint)");
            runner.execute("INSERT INTO redisearch.tpch.issue118_numbers VALUES (1), (2), (3)");
            assertThat(runner.execute("SELECT sum(id) FROM redisearch.tpch.issue118_numbers WHERE id > 1").getOnlyValue()).isEqualTo(5L);
            runner.execute("DROP TABLE redisearch.tpch.issue118_numbers");
            record("Software RAM numeric SQL", "CREATE / INSERT / numeric range / SUM / DROP passed");
            record("Software RAM SQL", "CREATE / INSERT / SELECT with filter / COUNT passed");
            record("Software RAM SHOW TABLES", runner.execute("SHOW TABLES FROM redisearch.tpch"));

            String missingError = failure(() -> runner.execute("CREATE TABLE missing.tpch.issue118_missing (id varchar, name varchar)"));
            record("missing Search CREATE TABLE", missingError);
            assertThat(missingError).containsIgnoringCase("unknown command").containsIgnoringCase("FT._LIST");
            record("missing Search SHOW TABLES", failure(() -> runner.execute("SHOW TABLES FROM missing.tpch")));

            raw(flex, "FT.CREATE", "issue118_flex", "ON", "HASH", "PREFIX", "1", "issue118_flex:", "SKIPINITIALSCAN", "SCHEMA", "id", "TAG");
            flex.sync().hset("issue118_flex:1", Map.of("id", "1"));
            String aggregateError = outcome(() -> raw(flex, "FT.AGGREGATE", "issue118_flex", "*", "LIMIT", "0", "0"));
            record("Software Flex FT.AGGREGATE", aggregateError);
            record("Software Flex FT.AGGREGATE LOAD", outcome(() -> raw(flex, "FT.AGGREGATE", "issue118_flex", "*", "LOAD", "1", "@id", "LIMIT", "0", "10")));
            record("Software Flex FT.AGGREGATE WITHCURSOR", outcome(() -> raw(flex, "FT.AGGREGATE", "issue118_flex", "*", "WITHCURSOR", "COUNT", "1")));
            record("Software Flex FT.SEARCH fields", outcome(() -> raw(flex, "FT.SEARCH", "issue118_flex", "*", "RETURN", "1", "id")));
            record("Software Flex SHOW TABLES", runner.execute("SHOW TABLES FROM flex.tpch"));
            record("Software Flex SELECT", outcome(() -> runner.execute("SELECT * FROM flex.tpch.issue118_flex")));
            runner.execute("CREATE TABLE flex.tpch.issue118_ddl (id varchar, name varchar)");
            record("Software Flex CREATE varchar TABLE", "succeeded");
            record("Software Flex CREATE numeric TABLE", outcome(() -> runner.execute("CREATE TABLE flex.tpch.issue118_numeric (id bigint)")));

            raw(acl, "ACL", "SETUSER", "issue118", "on", ">local-issue118", "~*", "+@all", "-ft._list");
            runner.createCatalog("restricted", "redisearch", Map.of("redisearch.uri", "redis://127.0.0.1:12021",
                    "redisearch.username", "issue118", "redisearch.password", "local-issue118", "redisearch.default-schema-name", "tpch"));
            String aclError = failure(() -> runner.execute("SHOW TABLES FROM restricted.tpch"));
            record("ACL denial SHOW TABLES", aclError);
            assertThat(aclError).contains("NOPERM");
            // Reserve a local port without listening to get a deterministic connection-refused control.
            try (java.net.ServerSocket reserved = new java.net.ServerSocket()) {
                reserved.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                int port = reserved.getLocalPort();
                reserved.close();
                io.trino.spi.type.TypeManager unusedTypes = (io.trino.spi.type.TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                        io.trino.spi.type.TypeManager.class.getClassLoader(), new Class<?>[] {io.trino.spi.type.TypeManager.class},
                        (proxy, method, args) -> { throw new UnsupportedOperationException(); });
                String connectionError = failure(() -> new RediSearchSession(unusedTypes,
                        new RediSearchConfig().setUri("redis://127.0.0.1:" + port)));
                record("connection refused", connectionError);
                assertThat(connectionError).contains("RedisConnectionException").doesNotContain("NOPERM");
            }
            raw(acl, "ACL", "DELUSER", "issue118");
            runner.execute("DROP TABLE redisearch.tpch.issue118_ram");
            raw(flex, "FT.DROPINDEX", "issue118_flex");
            raw(flex, "FT.DROPINDEX", "issue118_ddl");
            if (flex.sync().ftList().contains("issue118_numeric")) {
                raw(flex, "FT.DROPINDEX", "issue118_numeric");
            }
            flex.sync().del("__trino:columns:issue118_numeric", "__trino:columns:issue118_ddl");
            flex.sync().del("issue118_flex:1");
            record("result", "original diagnostics gap reproduced; Software RAM control passed");
        } finally {
            client.shutdown();
            client.getResources().shutdown();
        }
    }
}
