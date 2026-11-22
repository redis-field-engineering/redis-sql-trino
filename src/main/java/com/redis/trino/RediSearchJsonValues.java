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
    // Validate the modern selector forms whose DIALECT 3 LOAD / JSON.GET value parity is tested. FT.CREATE
    // alone is insufficient validation: it accepts some paths (e.g. $.café) which JSON.GET rejects.
    private static final String MEMBER = "[A-Za-z_][A-Za-z_0-9]*";
    private static final String QUOTED = "'(?:[^'\\\\\\p{Cntrl}]|\\\\['\\\\])*'|\"(?:[^\"\\\\\\p{Cntrl}]|\\\\[\"\\\\])*\"";
    private static final String INDEX = "(?:0|-?[1-9][0-9]*)";
    private static final String SLICE = "(?:" + INDEX + ")?:(?:" + INDEX + ")?(?::[1-9][0-9]*)?";
    private static final String BRACKET = "\\[(?:\\*|" + SLICE + "|" + INDEX + "(?:," + INDEX + ")*|(?:"
            + QUOTED + ")(?:,(?:" + QUOTED + "))*)\\]";
    private static final Pattern MODERN_PATH = Pattern.compile("\\$(?:(?:\\.{1,2}(?:" + MEMBER + "|\\*))|" + BRACKET + ")*");
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
                .filter(RediSearchJsonValues::isSupportedPath);
    }

    private static boolean isSupportedPath(String path) {
        if (!MODERN_PATH.matcher(path).matches()) { return false; }
        // Redis can accept an unparseable integer selector in FT.CREATE and reject it later in JSON.GET.
        // Bound numeric selectors to signed 32-bit values; keys containing digits remain unrestricted.
        boolean bracket = false;
        char quote = 0;
        for (int i = 0; i < path.length(); i++) {
            char next = path.charAt(i);
            if (quote != 0) {
                if (next == '\\') { i++; }
                else if (next == quote) { quote = 0; }
            } else if (bracket && (next == '\'' || next == '"')) { quote = next; }
            else if (next == '[') { bracket = true; }
            else if (next == ']') { bracket = false; }
            else if (bracket && (next == '-' || next >= '0' && next <= '9')) {
                int start = i++;
                while (i < path.length() && path.charAt(i) >= '0' && path.charAt(i) <= '9') { i++; }
                try { Integer.parseInt(path.substring(start, i)); }
                catch (NumberFormatException invalid) { return false; }
                i--;
            }
        }
        return true;
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
