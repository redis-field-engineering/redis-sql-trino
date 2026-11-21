package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class TestJsonWriter {
	@Test
	void testAliasesNestedNumbersAndNullPatch() {
		RediSearchJsonWriter writer = writer(field("name", "$.profile.name", RediSearchFieldType.TAG),
				field("score", "$.score", RediSearchFieldType.NUMERIC));
		assertThat(writer.document(Map.of("name", "quote\"\\\n", "score", "9007199254740993")))
				.contains("\"profile\":{\"name\":\"quote\\\"\\\\\\n\"}", "\"score\":9007199254740993");
		Map<String, String> patch = new LinkedHashMap<>();
		patch.put("name", null);
		assertThat(writer.document(patch)).isEqualTo("{\"profile\":{\"name\":null}}");
		assertThat(writer.document(Map.of())).isEqualTo("{}");
	}

	@Test
	void testAmbiguousAndUnmappedPathsAreRejected() {
		for (String path : List.of("$", "$..name", "$.names[*]", "$.a[0]", "$['a']")) {
			assertThatThrownBy(() -> writer(field("alias", path, RediSearchFieldType.TAG)))
					.hasMessageContaining("scalar object paths");
		}
		assertThatThrownBy(() -> writer(field("a", "$.a", RediSearchFieldType.TAG),
				field("b", "$.a.b", RediSearchFieldType.TAG))).hasMessageContaining("overlapping");
		assertThatThrownBy(() -> writer(field("a", "$.a", RediSearchFieldType.TAG),
				field("b", "$.a", RediSearchFieldType.TAG))).hasMessageContaining("overlapping");
		assertThatThrownBy(() -> writer(field("a", "$.a", RediSearchFieldType.TAG))
				.document(Map.of("$", "{}"))).hasMessageContaining("indexed scalar path");
	}

	@Test
	void testNonFiniteNumbersAreRejected() {
		RediSearchJsonWriter writer = writer(field("n", "$.n", RediSearchFieldType.NUMERIC));
		for (String value : List.of("NaN", "Infinity", "-Infinity")) {
			assertThatThrownBy(() -> writer.document(Map.of("n", value))).hasMessageContaining("finite number");
		}
	}

	private static RediSearchIndexInfo.Field field(String alias, String path, RediSearchFieldType type) {
		return new RediSearchIndexInfo.Field(alias, path, type, Optional.empty());
	}

	private static RediSearchJsonWriter writer(RediSearchIndexInfo.Field... fields) {
		return new RediSearchJsonWriter(new RediSearchIndexInfo(Optional.of(RediSearchIndexInfo.KeyType.JSON),
				List.of("doc:"), List.of(fields), false, 1, false));
	}
}
