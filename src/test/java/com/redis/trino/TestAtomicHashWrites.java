package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

/** Runs the write contract with opt-in single-key Lua updates. */
public class TestAtomicHashWrites extends TestWrites {
	@Override
	protected Map<String, String> writeProperties() {
		return Map.of("redisearch.atomic-hash-updates.enabled", "true");
	}

	@Test
	public void testAtomicHashNullAssignments() {
		assertUpdate("CREATE TABLE atomic_assignments (a bigint, b bigint)");
		try {
			assertUpdate("INSERT INTO atomic_assignments VALUES (1, 2)", 1);
			assertUpdate("UPDATE atomic_assignments SET a = NULL, b = 3", 1);
			assertThat(query("SELECT * FROM atomic_assignments")).matches("VALUES (CAST(NULL AS bigint), BIGINT '3')");
			assertUpdate("UPDATE atomic_assignments SET b = NULL", 1);
			assertThat(query("SELECT * FROM atomic_assignments")).matches("VALUES (CAST(NULL AS bigint), CAST(NULL AS bigint))");
			assertUpdate("UPDATE atomic_assignments SET a = 4, b = 5", 1);
			assertThat(query("SELECT * FROM atomic_assignments")).matches("VALUES (BIGINT '4', BIGINT '5')");
		} finally {
			assertUpdate("DROP TABLE atomic_assignments");
		}
	}
}
