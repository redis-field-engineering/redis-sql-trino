package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.TagFieldArgs;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

/**
 * Indexes changed outside Trino show up once the connector describes them again in the background.
 */
public class TestTableCacheRefresh extends AbstractTestQueryFramework {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private RediSearchServer redisearch;

	@Override
	protected QueryRunner createQueryRunner() throws Exception {
		redisearch = closeAfterClass(new RediSearchServer());
		QueryRunner queryRunner = RediSearchQueryRunner.createRediSearchQueryRunner(redisearch, List.of(), Map.of(),
				Map.of("redisearch.table-cache-refresh", "1"));
		// Without a cache
		queryRunner.createCatalog("uncached", "redisearch", ImmutableMap.<String, String> builder()
				.putAll(redisearch.getConnectorProperties()).put("redisearch.default-schema-name", "tpch")
				.put("redisearch.table-cache-refresh", "0").buildOrThrow());
		return queryRunner;
	}

	@Test
	public void testChangesOutsideTrino() {
		RedisCommands<String, String> redis = redisearch.getConnection().sync();
		redis.ftCreate("refreshed", CreateArgs.builder().withPrefix("refreshed:").build(),
				List.of(TagFieldArgs.builder().name("a").build()));
		redisearch.awaitIndexed("refreshed");
		redis.hset("refreshed:1", Map.of("a", "x", "b", "1"));
		// A field that isn't indexed, which the connector finds in the documents
		assertThat(type("redisearch", "b")).isEqualTo("varchar");
		redis.ftAlter("refreshed", List.of(NumericFieldArgs.builder().name("b").build()));
		redisearch.awaitIndexed("refreshed");
		// The uncached catalog describes the index for each query
		assertThat(type("uncached", "b")).isEqualTo("double");
		await(() -> type("redisearch", "b").equals("double"));
		assertThat(computeActual("SELECT b FROM refreshed").getOnlyValue()).isEqualTo(1.0);
		redis.ftDropindex("refreshed", true);
		// Until then, the query fails in Redis
		await(() -> failure("SELECT count(*) FROM refreshed").contains("does not exist"));
	}

	private String type(String catalog, String column) {
		return (String) computeActual("SELECT data_type FROM " + catalog + ".information_schema.columns "
				+ "WHERE table_schema = 'tpch' AND table_name = 'refreshed' AND column_name = '" + column + "'")
				.getOnlyValue();
	}

	private String failure(String sql) {
		try {
			computeActual(sql);
		} catch (RuntimeException e) {
			return String.valueOf(e.getMessage());
		}
		throw new AssertionError("Query succeeded: " + sql);
	}

	private static void await(BooleanSupplier condition) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (!condition.getAsBoolean()) {
			assertThat(System.nanoTime()).as("condition within %s", TIMEOUT).isLessThan(deadline);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}
	}
}
