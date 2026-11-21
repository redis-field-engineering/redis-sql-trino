package com.redis.trino;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorViewDefinition;

/**
 * Versioned, bounded string metadata. Strings cannot become document rows even on indexes without prefixes.
 * Every mutation compares and replaces one key atomically, including on Redis Cluster. No keyspace scan is needed.
 */
final class RediSearchRelationMetadata {

    private static final Logger log = Logger.get(RediSearchRelationMetadata.class);
    private static final JsonCodec<Map<String, String>> MAP_CODEC = JsonCodec.mapJsonCodec(String.class, String.class);
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC = JsonCodec.jsonCodec(ConnectorViewDefinition.class);
    private static final int MAX_METADATA_BYTES = 1024 * 1024;
    private static final String COMPARE_AND_SET = "local current = redis.call('GET', KEYS[1]); "
            + "if (ARGV[1] == 'absent' and current == false) or (ARGV[1] == 'present' and current == ARGV[2]) then "
            + "if ARGV[3] == '{}' then redis.call('DEL', KEYS[1]) else redis.call('SET', KEYS[1], ARGV[3]) end; "
            + "return 1 end; return 0";

    private final RedisClusterCommands<String, String> redis;
    private final Map<String, Map<String, String>> readCache = new java.util.concurrent.ConcurrentHashMap<>();

    RediSearchRelationMetadata(RedisClusterCommands<String, String> redis) {
        this.redis = redis;
    }

    static String viewsKey(String schema) {
        return "__trino:views:v1:" + schema;
    }

    static String commentsKey(String index) {
        return "__trino:comments:v1:" + index;
    }

    java.util.Set<String> viewNames(String schema) {
        return java.util.Set.copyOf(read(viewsKey(schema)).keySet());
    }

    Optional<ConnectorViewDefinition> view(String schema, String name) {
        return Optional.ofNullable(read(viewsKey(schema)).get(name)).map(VIEW_CODEC::fromJson);
    }

    Map<String, ConnectorViewDefinition> views(String schema) {
        Map<String, ConnectorViewDefinition> views = new LinkedHashMap<>();
        read(viewsKey(schema)).forEach((name, json) -> views.put(name, VIEW_CODEC.fromJson(json)));
        return views;
    }

    void createView(String schema, String name, ConnectorViewDefinition definition, boolean replace, boolean ignoreExisting) {
        update(viewsKey(schema), values -> {
            if (values.containsKey(name) && !replace) {
                if (ignoreExisting) {
                    return;
                }
                throw new TrinoException(StandardErrorCode.ALREADY_EXISTS, "View already exists: " + schema + "." + name);
            }
            values.put(name, VIEW_CODEC.toJson(definition));
        });
    }

    void dropView(String schema, String name) {
        update(viewsKey(schema), values -> {
            if (values.remove(name) == null) {
                throw new io.trino.spi.connector.ViewNotFoundException(new io.trino.spi.connector.SchemaTableName(schema, name));
            }
        });
    }

    void renameView(String schema, String source, String target) {
        update(viewsKey(schema), values -> {
            if (!values.containsKey(source)) {
                throw new io.trino.spi.connector.ViewNotFoundException(new io.trino.spi.connector.SchemaTableName(schema, source));
            }
            if (values.containsKey(target)) {
                throw new TrinoException(StandardErrorCode.ALREADY_EXISTS, "View already exists: " + schema + "." + target);
            }
            values.put(target, values.remove(source));
        });
    }

    void updateView(String schema, String name, java.util.function.UnaryOperator<ConnectorViewDefinition> update) {
        update(viewsKey(schema), values -> {
            if (!values.containsKey(name)) {
                throw new io.trino.spi.connector.ViewNotFoundException(new io.trino.spi.connector.SchemaTableName(schema, name));
            }
            values.put(name, VIEW_CODEC.toJson(update.apply(VIEW_CODEC.fromJson(values.get(name)))));
        });
    }

    Map<String, String> comments(String index) {
        return read(commentsKey(index));
    }

    void initializeComments(String index, Map<String, String> comments) {
        if (!comments.isEmpty()) {
            // Validate the entire replacement in update() before its single atomic write.
            update(commentsKey(index), values -> values.putAll(comments));
        }
    }

    void setComment(String index, Optional<String> column, Optional<String> comment) {
        String field = column.map(name -> "column:" + name).orElse("table");
        update(commentsKey(index), values -> {
            if (comment.isPresent()) {
                values.put(field, comment.orElseThrow());
            }
            else {
                values.remove(field);
            }
        });
    }

    void deleteComments(String index) {
        redis.del(commentsKey(index));
        readCache.remove(commentsKey(index));
    }

    private Map<String, String> read(String key) {
        return readCache.computeIfAbsent(key, this::load);
    }

    private Map<String, String> load(String key) {
        try {
            String value = redis.get(key);
            return value == null ? Map.of() : MAP_CODEC.fromJson(value);
        }
        catch (RedisCommandExecutionException e) {
            // Existing deployments may restrict metadata-key ACLs. Discovery remains usable; mutations fail explicitly.
            if (e.getMessage().startsWith("NOPERM")) {
                log.warn("Cannot read optional relation metadata %s: %s", key, e.getMessage());
                return Map.of();
            }
            throw e;
        }
    }

    private void update(String key, Consumer<Map<String, String>> change) {
        for (int attempt = 0; attempt < 8; attempt++) {
            String previous = redis.get(key);
            Map<String, String> values = previous == null ? new LinkedHashMap<>() : new LinkedHashMap<>(MAP_CODEC.fromJson(previous));
            change.accept(values);
            String replacement = MAP_CODEC.toJson(values);
            if (replacement.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_METADATA_BYTES) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Relation metadata exceeds the 1 MiB per-key limit: " + key);
            }
            Long changed = redis.eval(COMPARE_AND_SET, ScriptOutputType.INTEGER, new String[] {key},
                    previous == null ? "absent" : "present", previous == null ? "" : previous, replacement);
            if (changed == 1L) {
                readCache.remove(key);
                return;
            }
        }
        throw new TrinoException(StandardErrorCode.TRANSACTION_CONFLICT, "Concurrent relation metadata updates: " + key);
    }
}
