package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.search.AggregationReply;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.TrinoException;

class TestQueryErrors {
    private static final String RESPONSE = "ERR could not perform command 'ft.aggregate' because "
            + "shards topology update is either transient or has failed";

    @Test
    void classifiesServerTopologyFailureAndPreservesResponse() {
        RedisCommandExecutionException server = new RedisCommandExecutionException(RESPONSE);
        for (Throwable failure : new Throwable[] {server, new CompletionException(server),
                new CompletionException(new ExecutionException(server))}) {
            RuntimeException result = RediSearchQueryErrors.classify(failure);
            assertThat(result).isInstanceOf(TrinoException.class).hasCause(server).hasMessageContaining(RESPONSE);
            assertThat(((TrinoException) result).getErrorCode())
                    .isEqualTo(RediSearchErrorCode.REDISEARCH_SHARD_TOPOLOGY_UNAVAILABLE.toErrorCode());
        }
    }

    @Test
    void preservesParserTimeoutAndOtherFailures() {
        for (RuntimeException failure : new RuntimeException[] {
                new RedisCommandExecutionException("SEARCH_PARSE_ARGS Bad arguments for PARAMS"),
                new RedisCommandTimeoutException("Command timed out"),
                new IllegalStateException(RESPONSE)}) {
            assertThat(RediSearchQueryErrors.classify(new CompletionException(failure))).isSameAs(failure);
        }
        AssertionError error = new AssertionError("failure");
        assertThatThrownBy(() -> RediSearchQueryErrors.classify(new CompletionException(error))).isSameAs(error);
    }

    @Test
    void recordsFailedRequestWithoutRetrying() {
        RediSearchReadStats stats = new RediSearchReadStats();
        AtomicInteger requests = new AtomicInteger();
        assertThatThrownBy(() -> stats.redisRequest(() -> {
            requests.incrementAndGet();
            throw new RedisCommandExecutionException(RESPONSE);
        })).isInstanceOf(TrinoException.class);
        assertThat(requests.get()).isEqualTo(1);
        assertThat(stats.requestNanos.sum()).isPositive();
    }
    @Test
    void asyncCursorFailureIsClassifiedWithoutRetrying() {
        RediSearchReadStats stats = new RediSearchReadStats();
        CompletableFuture<String> server = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        CompletableFuture<String> read = stats.redisRequestAsync(() -> {
            requests.incrementAndGet();
            return server;
        });
        assertThat(read.isDone()).isFalse();
        server.completeExceptionally(new RedisCommandExecutionException(RESPONSE));
        assertThatThrownBy(read::join).isInstanceOf(CompletionException.class).hasCauseInstanceOf(TrinoException.class);
        assertThat(requests.get()).isEqualTo(1);
        assertThat(stats.requestNanos.sum()).isPositive();
    }

    @Test
    void cursorCleanupWaitsForFailedReadAndHandlesRemovedFailedRead() {
        AtomicInteger deletions = new AtomicInteger();
        CompletableFuture<AggregationReply<String>> read = new CompletableFuture<>();
        RediSearchPageSource.deleteAfterReads(List.of(read), deletions::incrementAndGet);
        assertThat(deletions.get()).isZero();
        read.completeExceptionally(new RedisCommandExecutionException(RESPONSE));
        assertThat(deletions.get()).isEqualTo(1);
        // getNextSourcePage removes a completed failed future before it reports the error.
        RediSearchPageSource.deleteAfterReads(List.of(), deletions::incrementAndGet);
        assertThat(deletions.get()).isEqualTo(2);
        AggregationReply<String> exhausted = new AggregationReply<>();
        exhausted.setCursor(Cursor.of(0, "node"));
        RediSearchPageSource.deleteAfterReads(List.of(CompletableFuture.completedFuture(exhausted)), deletions::incrementAndGet);
        assertThat(deletions.get()).isEqualTo(2);
    }

}
