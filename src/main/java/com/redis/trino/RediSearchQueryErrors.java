package com.redis.trino;

import static com.redis.trino.RediSearchErrorCode.REDISEARCH_SHARD_TOPOLOGY_UNAVAILABLE;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import io.lettuce.core.RedisCommandExecutionException;
import io.trino.spi.TrinoException;

/** Classifies the server's explicit topology failure without retrying or replacing other failures. */
final class RediSearchQueryErrors {
    private static final String TOPOLOGY_ERROR = "shards topology update is either transient or has failed";

    private RediSearchQueryErrors() {}

    static RuntimeException classify(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof RedisCommandExecutionException && cause.getMessage() != null
                && cause.getMessage().contains(TOPOLOGY_ERROR)) {
            return new TrinoException(REDISEARCH_SHARD_TOPOLOGY_UNAVAILABLE,
                    "Redis Query Engine could not execute the query because its shard topology is unavailable. "
                            + "Check coordinator/shard health and compare a fresh direct connection before rerunning "
                            + "the complete query. Cursor reads are not retried because they may have consumed rows. "
                            + "Server response: " + cause.getMessage(), cause);
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return cause instanceof RuntimeException runtime ? runtime : new CompletionException(cause);
    }
}
