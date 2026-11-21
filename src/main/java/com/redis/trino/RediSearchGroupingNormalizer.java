package com.redis.trino;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.trino.spi.type.Type;
import io.trino.spi.type.CharType;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;

/** Canonical SQL grouping keys, kept separate from reducer inputs. */
final class RediSearchGroupingNormalizer {
    private RediSearchGroupingNormalizer() {}

    static boolean requiresNormalization(Type type) {
        return type.equals(DOUBLE) || type.equals(BOOLEAN)
                || type instanceof CharType character && character.getLength() <= 8;
    }

    /**
     * Creates a stage-local plan without modifying the caller's reserved-name set.
     * The caller must reserve the generated expression names before planning another stage.
     */
    static Plan plan(List<RediSearchAggregationTerm> terms, Set<String> reservedNames) {
        Map<String, String> aliases = new LinkedHashMap<>();
        Map<String, String> before = new LinkedHashMap<>();
        Map<String, String> after = new LinkedHashMap<>();
        Set<String> used = new LinkedHashSet<>(reservedNames);
        terms.forEach(term -> used.add(term.getTerm()));
        for (RediSearchAggregationTerm term : terms) {
            if (!requiresNormalization(term.getType()) || aliases.containsKey(term.getTerm())) {
                continue;
            }
            String field = term.getTerm();
            if (!RediSearchQueryBuilder.isProperty(field)) {
                throw new IllegalArgumentException("Grouping normalization requires a scalar field name: " + field);
            }
            String alias = allocate("__trino_group" + aliases.size(), used);
            if (term.getType() instanceof CharType character) {
                characterExpressions(field, alias, character.getLength(), used, before);
                aliases.put(field, alias);
                after.put(field, "case(exists(@" + alias + "), @" + alias + ", NULL)");
                continue;
            }
            String canonical = term.getType().equals(BOOLEAN)
                    // Boolean.parseBoolean returns true only for an ASCII case variant of "true".
                    // Every other present string, including empty/junk strings, is SQL false.
                    ? "case(strlen(@" + field + ") == 4 && lower(@" + field + ") == \"true\", \"true\", \"false\")"
                    // Redis hashes the signs of zero separately; SQL DOUBLE equality does not.
                    : "@" + field + " + 0";
            aliases.put(field, alias);
            before.put(alias, "case(exists(@" + field + "), " + canonical + ", NULL)");
            after.put(field, "case(exists(@" + alias + "), @" + alias + ", NULL)");
        }
        return new Plan(aliases, before, after);
    }

    private static String allocate(String candidate, Set<String> used) {
        while (!used.add(candidate)) {
            candidate += "_";
        }
        return candidate;
    }

    /**
     * Redis substr/strlen count bytes; SQL CHAR truncates Unicode code points and trims ASCII spaces.
     * A valid UTF-8 code point's width follows from its lexical position relative to U+0080/U+0800/U+10000.
     * Only a four-byte prefix is inspected, so a very large source string is never copied per character.
     */
    private static void characterExpressions(String field, String alias, int length, Set<String> used, Map<String, String> before) {
        String offset = "0";
        String trimmedEnd = "0";
        String source = "@" + field;
        for (int i = 0; i < length; i++) {
            String next = allocate(alias + "_offset" + i, used);
            String end = allocate(alias + "_end" + i, used);
            String prefix = "substr(" + source + ", " + offset + ", 4)";
            String width = "case(" + prefix + " < \"\u0080\", 1, case(" + prefix + " < \"\u0800\", 2, case("
                    + prefix + " < \"\uD800\uDC00\", 3, 4)))";
            String hasCharacter = offset + " < strlen(" + source + ")";
            before.put(next, "case(exists(" + source + "), case(" + hasCharacter + ", " + offset + " + " + width + ", " + offset + "), NULL)");
            before.put(end, "case(exists(" + source + "), case(" + hasCharacter + ", case(substr(" + source + ", " + offset
                    + ", @" + next + " - " + offset + ") == \" \", " + trimmedEnd + ", @" + next + "), " + trimmedEnd + "), NULL)");
            offset = "@" + next;
            trimmedEnd = "@" + end;
        }
        before.put(alias, "case(exists(" + source + "), substr(" + source + ", 0, " + trimmedEnd + "), NULL)");
    }

    record Plan(Map<String, String> aliases, Map<String, String> beforeExpressions, Map<String, String> afterExpressions) {
        Plan {
            aliases = immutableOrdered(aliases);
            beforeExpressions = immutableOrdered(beforeExpressions);
            afterExpressions = immutableOrdered(afterExpressions);
        }

        private static Map<String, String> immutableOrdered(Map<String, String> values) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }
    }
}
