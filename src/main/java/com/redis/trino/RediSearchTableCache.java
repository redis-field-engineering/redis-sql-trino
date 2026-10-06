/*
 * MIT License
 *
 * Copyright (c) 2022, Redis Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.redis.trino;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

/**
 * The tables the connector has described. A read of a table described {@code refresh} ago or more describes it again
 * in the background, and gets the cached table meanwhile, so that queries don't wait for Redis to plan. A table that
 * isn't read expires {@link #EXPIRATION_FACTOR} times as long after it was described, and the next read waits for it.
 */
final class RediSearchTableCache {

	static final int EXPIRATION_FACTOR = 10;

	private final LoadingCache<SchemaTableName, RediSearchTable> tables;

	/**
	 * @param refresh  how old a table can be before a read describes it again; 0 caches nothing
	 * @param loader   describes a table, or throws {@link TableNotFoundException}
	 * @param listed   whether Redis still lists a table's index, which tells a dropped index from a failure to
	 *                 describe it, such as a timeout, that the loader also reports as not found
	 * @param executor describes tables again in the background
	 */
	RediSearchTableCache(Duration refresh, Function<SchemaTableName, RediSearchTable> loader,
			Predicate<SchemaTableName> listed, Executor executor, Ticker ticker) {
		requireNonNull(loader, "loader is null");
		requireNonNull(listed, "listed is null");
		requireNonNull(executor, "executor is null");
		EvictableCacheBuilder<Object, Object> builder = EvictableCacheBuilder.newBuilder().ticker(ticker);
		if (refresh.isZero()) {
			builder.expireAfterWrite(Duration.ZERO).shareNothingWhenDisabled();
		} else {
			builder.refreshAfterWrite(refresh).expireAfterWrite(refresh.multipliedBy(EXPIRATION_FACTOR));
		}
		this.tables = builder.build(new CacheLoader<SchemaTableName, RediSearchTable>() {
			@Override
			public RediSearchTable load(SchemaTableName tableName) {
				return loader.apply(tableName);
			}

			@Override
			public ListenableFuture<RediSearchTable> reload(SchemaTableName tableName, RediSearchTable table) {
				ListenableFutureTask<RediSearchTable> task = ListenableFutureTask.create(() -> {
					try {
						return loader.apply(tableName);
					} catch (TableNotFoundException e) {
						// A failure, such as a timeout, keeps the cached table, and so does a failure to list the indexes
						if (listed.test(tableName)) {
							throw e;
						}
						// Dropped outside Trino: forgotten, so that the next read doesn't find it either
						invalidate(tableName);
						return table;
					}
				});
				// Other failures leave the cached table until it expires
				executor.execute(task);
				return task;
			}
		});
	}

	/**
	 * @throws TableNotFoundException if no index by that name was found
	 */
	RediSearchTable get(SchemaTableName tableName) {
		try {
			return tables.get(tableName);
		} catch (ExecutionException | UncheckedExecutionException e) {
			throwIfInstanceOf(e.getCause(), TrinoException.class);
			throw new RuntimeException(e);
		}
	}

	void invalidate(SchemaTableName tableName) {
		tables.invalidate(tableName);
	}
}
