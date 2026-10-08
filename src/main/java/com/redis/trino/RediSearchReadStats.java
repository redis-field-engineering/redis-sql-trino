package com.redis.trino;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.DurationTiming;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metrics;

/** Per-page-source counters, including cursor completions on Lettuce's connection thread. */
final class RediSearchReadStats {

	final LongAdder aggregateRequests = new LongAdder();
	final LongAdder cursorRequests = new LongAdder();
	final LongAdder receivedRows = new LongAdder();
	final LongAdder exactHashReads = new LongAdder();
	final LongAdder exactHashReadBatches = new LongAdder();
	final LongAdder requestNanos = new LongAdder();
	final LongAdder exactWaitNanos = new LongAdder();
	final LongAdder conversionNanos = new LongAdder();

	<T> T redisRequest(Supplier<T> request) {
		long start = System.nanoTime();
		try {
			return request.get();
		} catch (RuntimeException e) {
			throw RediSearchQueryErrors.classify(e);
		} finally {
			requestNanos.add(System.nanoTime() - start);
		}
	}

	<T> CompletableFuture<T> redisRequestAsync(Supplier<CompletableFuture<T>> request) {
		long start = System.nanoTime();
		try {
			return request.get().handle((reply, failure) -> {
				requestNanos.add(System.nanoTime() - start);
				if (failure != null) {
					throw RediSearchQueryErrors.classify(failure);
				}
				return reply;
			});
		} catch (RuntimeException e) {
			requestNanos.add(System.nanoTime() - start);
			throw RediSearchQueryErrors.classify(e);
		}
	}

	long readTimeNanos() {
		return requestNanos.sum() + exactWaitNanos.sum();
	}

	Metrics metrics() {
		return new Metrics(Map.of(
				"redis.aggregate.requests", new LongCount(aggregateRequests.sum()),
				"redis.cursor.requests", new LongCount(cursorRequests.sum()),
				"redis.rows.received", new LongCount(receivedRows.sum()),
				"redis.exact-hash-reads", new LongCount(exactHashReads.sum()),
				"redis.exact-hash-read-batches", new LongCount(exactHashReadBatches.sum()),
				"redis.request-wall-time", timing(requestNanos.sum()),
				"redis.exact-hash-read-wait-time", timing(exactWaitNanos.sum()),
				"redis.row-conversion-time", timing(conversionNanos.sum())));
	}

	private static DurationTiming timing(long nanos) {
		return new DurationTiming(new Duration(nanos, NANOSECONDS));
	}
}
