package com.redis.trino;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/** JSON.GET values with the same scalar/NULL/first-match representation as DIALECT 3 document scans. */
final class RediSearchJsonValues {
    private static final Pattern OBJECT_PATH = Pattern.compile("\\$(\\.[A-Za-z_][A-Za-z_0-9]*)+");
    private static final JsonFactory JSON = new JsonFactory();

    private RediSearchJsonValues() {}

    static boolean supportsSchema(RediSearchTable table) {
        return table.getIndexInfo().getKeyType().filter(RediSearchIndexInfo.KeyType.JSON::equals).isPresent()
                && table.getColumns().stream().allMatch(column -> RediSearchBuiltinField.isKeyColumn(column.getName())
                        || path(table.getIndexInfo(), column.getName()).isPresent());
    }

    static Optional<String> path(RediSearchIndexInfo info, String column) {
        return info.getFields().stream().filter(field -> field.getAttribute().equals(column))
                .map(RediSearchIndexInfo.Field::getIdentifier).findFirst()
                .or(() -> column.equals("$") ? Optional.of("$") : Optional.empty())
                .filter(value -> value.equals("$") || OBJECT_PATH.matcher(value).matches());
    }

    static Map<String, String> decode(String raw, List<String> paths) {
        if (raw == null) { return Map.of(); } // Document disappeared between selection and hydration.
        if (paths.size() == 1) {
            validateArray(raw);
            Map<String, String> values = new LinkedHashMap<>();
            values.put(paths.getFirst(), RediSearchRowReader.firstValue(raw).orElse(null));
            return Collections.unmodifiableMap(values);
        }
        Map<String, String> values = new LinkedHashMap<>();
        try (JsonParser parser = JSON.createParser(raw)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) { throw invalid(); }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) { throw invalid(); }
                String path = parser.currentName();
                if (!paths.contains(path) || values.containsKey(path) || parser.nextToken() != JsonToken.START_ARRAY) {
                    throw invalid();
                }
                int start = Math.toIntExact(parser.currentTokenLocation().getCharOffset());
                parser.skipChildren();
                int end = Math.toIntExact(parser.currentLocation().getCharOffset());
                values.put(path, RediSearchRowReader.firstValue(raw.substring(start, end)).orElse(null));
            }
            if (values.size() != paths.size() || parser.nextToken() != null) { throw invalid(); }
            return Collections.unmodifiableMap(values);
        } catch (IOException failure) {
            throw new UncheckedIOException("Invalid JSON.GET reply", failure);
        }
    }

    private static void validateArray(String raw) {
        try (JsonParser parser = JSON.createParser(raw)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) { throw invalid(); }
            parser.skipChildren();
            if (parser.nextToken() != null) { throw invalid(); }
        } catch (IOException failure) {
            throw new UncheckedIOException("Invalid JSON.GET reply", failure);
        }
    }

    private static IllegalArgumentException invalid() { return new IllegalArgumentException("Invalid JSON.GET reply"); }
}
