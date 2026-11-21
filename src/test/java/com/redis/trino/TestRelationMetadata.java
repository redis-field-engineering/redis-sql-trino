package com.redis.trino;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRelationMetadata {

    @Test
    void testViewRoundTripAndLifecycle() {
        RediSearchRelationMetadata metadata = new RediSearchRelationMetadata(redis(new HashMap<>()));
        ConnectorViewDefinition definition = new ConnectorViewDefinition("SELECT 1 AS id", Optional.of("redis"),
                Optional.of("default"), List.of(ConnectorViewDefinition.ViewColumn.fromType("id", BigintType.BIGINT, Optional.of("identifier"))),
                Optional.of("view comment"), Optional.of("alice"), false, List.of(new CatalogSchemaName("redis", "default")));
        metadata.createView("default", "v", definition, false, false);
        ConnectorViewDefinition restored = metadata.views("default").get("v");
        assertThat(restored.getOriginalSql()).isEqualTo(definition.getOriginalSql());
        assertThat(restored.getOwner()).contains("alice");
        assertThat(restored.getComment()).contains("view comment");
        assertThat(restored.getColumns().getFirst().getType()).isEqualTo(BigintType.BIGINT.getTypeId());
        assertThat(restored.getColumns().getFirst().getComment()).contains("identifier");
        assertThat(restored.getPath()).containsExactly(new CatalogSchemaName("redis", "default"));
        assertThatThrownBy(() -> metadata.createView("default", "v", definition, false, false)).hasMessageContaining("already exists");
        metadata.createView("default", "v", definition, false, true);
        metadata.createView("default", "v", definition, true, false);
        metadata.dropView("default", "v");
        assertThat(metadata.views("default")).isEmpty();
        assertThatThrownBy(() -> metadata.dropView("default", "v")).isInstanceOf(ViewNotFoundException.class);
    }

    @Test
    void testCommentsDoNotOverwriteOtherCommentsOrTypeMetadata() {
        Map<String, String> values = new HashMap<>();
        values.put(RediSearchColumnTypes.key("t"), "{\"id\":\"bigint\"}");
        RediSearchRelationMetadata metadata = new RediSearchRelationMetadata(redis(values));
        metadata.setComment("t", Optional.empty(), Optional.of("table comment"));
        metadata.setComment("t", Optional.of("id"), Optional.of("identifier"));
        metadata.setComment("t", Optional.of("table"), Optional.of("column named table"));
        metadata.setComment("t", Optional.empty(), Optional.empty());
        assertThat(metadata.comments("t")).containsExactlyInAnyOrderEntriesOf(Map.of("column:id", "identifier", "column:table", "column named table"));
        assertThat(values.get(RediSearchColumnTypes.key("t"))).isEqualTo("{\"id\":\"bigint\"}");
        metadata.deleteComments("t");
        assertThat(metadata.comments("t")).isEmpty();
    }

    @Test
    void testInitialCommentsUseOneAtomicMutationAndValidateTotalSize() {
        Map<String, String> values = new HashMap<>();
        RedisClusterCommands<String, String> backing = redis(values);
        int[] mutations = {0};
        @SuppressWarnings("unchecked")
        RedisClusterCommands<String, String> counted = (RedisClusterCommands<String, String>) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {RedisClusterCommands.class}, (proxy, method, args) -> {
                    if (method.getName().equals("eval")) {
                        mutations[0]++;
                    }
                    return method.invoke(backing, args);
                });
        RediSearchRelationMetadata metadata = new RediSearchRelationMetadata(counted);
        metadata.initializeComments("t", Map.of("table", "table comment", "column:a", "a", "column:b", "b"));
        assertThat(mutations[0]).isEqualTo(1);
        assertThat(metadata.comments("t")).hasSize(3);
        assertThatThrownBy(() -> metadata.initializeComments("large", Map.of("table", "x".repeat(1024 * 1024))))
                .hasMessageContaining("1 MiB");
        assertThat(mutations[0]).isEqualTo(1);
        assertThat(values).doesNotContainKey(RediSearchRelationMetadata.commentsKey("large"));
    }

    @Test
    void testConcurrentCommentUpdatesRetryWithoutLosingFields() {
        Map<String, String> values = new HashMap<>();
        RedisClusterCommands<String, String> backing = redis(values);
        boolean[] injected = {false};
        @SuppressWarnings("unchecked")
        RedisClusterCommands<String, String> concurrent = (RedisClusterCommands<String, String>) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {RedisClusterCommands.class}, (proxy, method, args) -> {
                    if (method.getName().equals("eval") && !injected[0]) {
                        injected[0] = true;
                        values.put(RediSearchRelationMetadata.commentsKey("t"), "{\"column:other\":\"concurrent\"}");
                        return 0L;
                    }
                    return method.invoke(backing, args);
                });
        new RediSearchRelationMetadata(concurrent).setComment("t", Optional.empty(), Optional.of("ours"));
        assertThat(new RediSearchRelationMetadata(backing).comments("t"))
                .containsExactlyInAnyOrderEntriesOf(Map.of("column:other", "concurrent", "table", "ours"));
    }

    @SuppressWarnings("unchecked")
    private static RedisClusterCommands<String, String> redis(Map<String, String> values) {
        return (RedisClusterCommands<String, String>) Proxy.newProxyInstance(TestRelationMetadata.class.getClassLoader(),
                new Class<?>[] {RedisClusterCommands.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "get" -> values.get((String) args[0]);
                    case "del" -> {
                        long removed = 0;
                        for (String key : (String[]) args[0]) {
                            if (values.remove(key) != null) {
                                removed++;
                            }
                        }
                        yield removed;
                    }
                    case "eval" -> {
                        String key = ((String[]) args[2])[0];
                        String[] arguments = (String[]) args[3];
                        String previous = values.get(key);
                        boolean matches = arguments[0].equals("absent") ? previous == null : arguments[1].equals(previous);
                        if (!matches) {
                            yield 0L;
                        }
                        if (arguments[2].equals("{}")) {
                            values.remove(key);
                        }
                        else {
                            values.put(key, arguments[2]);
                        }
                        yield 1L;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
