package com.redis.trino;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.airlift.testing.TestingTicker;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.VarcharType;

public class TestTableCache {

	private static final SchemaTableName BEERS = new SchemaTableName("tpch", "beers");

	private final TestingTicker ticker = new TestingTicker();

	// The reloads the cache has started, which run when the test runs them
	private final List<Runnable> reloads = new ArrayList<>();

	private final AtomicInteger loads = new AtomicInteger();

	// Each load describes the table with one more column than the last
	private final Function<SchemaTableName, RediSearchTable> loader = tableName -> table(loads.incrementAndGet());

	@Test
	public void testRefreshesInBackground() {
		RediSearchTableCache cache = cache(Duration.ofSeconds(60), loader);
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		ticker.increment(60, SECONDS);
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		assertThat(reloads).isEmpty();
		ticker.increment(1, SECONDS);
		// Older than the refresh: reads keep the cached table while it's described again, once
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		assertThat(reloads).hasSize(1);
		reloads.remove(0).run();
		assertThat(columns(cache.get(BEERS))).isEqualTo(2);
		assertThat(reloads).isEmpty();
	}

	@Test
	public void testExpiresUnread() {
		RediSearchTableCache cache = cache(Duration.ofSeconds(60), loader);
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		ticker.increment(60 * RediSearchTableCache.EXPIRATION_FACTOR, SECONDS);
		// Expired, so the read waits for the table
		assertThat(columns(cache.get(BEERS))).isEqualTo(2);
		assertThat(reloads).isEmpty();
	}

	@Test
	public void testForgetsDroppedTable() {
		RediSearchTableCache cache = cache(Duration.ofSeconds(60), tableName -> {
			if (loads.incrementAndGet() > 1) {
				throw new TableNotFoundException(tableName);
			}
			return table(1);
		});
		cache.get(BEERS);
		ticker.increment(61, SECONDS);
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		reloads.remove(0).run();
		assertThatThrownBy(() -> cache.get(BEERS)).isInstanceOf(TableNotFoundException.class);
	}

	@Test
	public void testKeepsTableWhenRefreshFails() {
		RediSearchTableCache cache = cache(Duration.ofSeconds(60), tableName -> {
			if (loads.incrementAndGet() == 2) {
				throw new IllegalStateException("Redis is unavailable");
			}
			return table(loads.get());
		});
		cache.get(BEERS);
		ticker.increment(61, SECONDS);
		cache.get(BEERS);
		reloads.remove(0).run();
		// The next read tries again
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		reloads.remove(0).run();
		assertThat(columns(cache.get(BEERS))).isEqualTo(3);
	}

	@Test
	public void testDisabled() {
		RediSearchTableCache cache = cache(Duration.ZERO, loader);
		assertThat(columns(cache.get(BEERS))).isEqualTo(1);
		assertThat(columns(cache.get(BEERS))).isEqualTo(2);
		assertThat(reloads).isEmpty();
	}

	@Test
	public void testInvalidate() {
		RediSearchTableCache cache = cache(Duration.ofSeconds(60), loader);
		cache.get(BEERS);
		cache.invalidate(BEERS);
		assertThat(columns(cache.get(BEERS))).isEqualTo(2);
	}

	private RediSearchTableCache cache(Duration refresh, Function<SchemaTableName, RediSearchTable> loader) {
		return new RediSearchTableCache(refresh, loader, reloads::add, ticker);
	}

	private static RediSearchTable table(int columns) {
		return new RediSearchTable(new RediSearchTableHandle(BEERS, "beers"),
				IntStream.range(0, columns).mapToObj(i -> new RediSearchColumnHandle("c" + i, VarcharType.VARCHAR,
						RediSearchFieldType.TAG, false, true, Optional.empty())).toList(),
				new RediSearchIndexInfo(Optional.of(RediSearchIndexInfo.KeyType.HASH), List.of("beer:"), List.of(), false,
						1, false));
	}

	private static int columns(RediSearchTable table) {
		return table.getColumns().size();
	}
}
