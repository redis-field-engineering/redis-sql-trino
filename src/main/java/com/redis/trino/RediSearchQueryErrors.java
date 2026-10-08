package com.redis.trino;

import static com.redis.trino.RediSearchErrorCode.REDISEARCH_SHARD_TOPOLOGY_UNAVAILABLE;
import static com.redis.trino.RediSearchErrorCode.REDISEARCH_INCOMPLETE_RESULT;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.search.AggregationReply;
import io.trino.spi.TrinoException;

/** Preserves server failures and rejects warned results without retrying commands. */
final class RediSearchQueryErrors {
    private static final String TOPOLOGY_ERROR = "shards topology update is either transient or has failed";

    private RediSearchQueryErrors() {}

    static void verifyComplete(AggregationReply<String> reply) {
        for (var shard : reply.getReplies()) {
            if (!shard.getWarnings().isEmpty()) {
                throw new TrinoException(REDISEARCH_INCOMPLETE_RESULT,
                        "Redis Query Engine returned query warnings; refusing a possibly incomplete result. "
                                + "Check server health before rerunning the complete query. Server warnings: "
                                + String.join("; ", shard.getWarnings()));
            }
        }
    }

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
