package com.redis.trino;

import static com.redis.trino.RediSearchErrorCode.REDISEARCH_CURSOR_REPLY_LOST;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.trino.spi.TrinoException;

/** A lost cursor reply fails the query; only idempotent cursor deletion gets a bounded retry. */
final class RediSearchCursorRecovery {
    private RediSearchCursorRecovery() {}

    static ClientOptions.Builder configure(ClientOptions.Builder builder) {
        // The replay filter covers failed writes. REJECT_COMMANDS also cancels the pending-command queue
        // when a connection drops after a write succeeded; that path bypasses the driver's replay filter.
        // Connections still reconnect, so subsequent complete queries can use them again.
        return builder.disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .replayFilter(RediSearchCursorRecovery::discardCursorRead);
    }

    private static boolean discardCursorRead(RedisCommand<?, ?, ?> command) {
        if (command.getType() == CommandType.FT_CURSOR && command.getArgs() != null
                && command.getArgs().toCommandString().regionMatches(true, 0, "READ ", 0, 5)) {
            command.completeExceptionally(lostReply(null));
            return true;
        }
        return false;
    }

    static RuntimeException readFailure(Throwable failure) {
        RuntimeException cause = RediSearchQueryErrors.classify(failure);
        RuntimeException nested = driverCause(cause);
        if (nested instanceof TrinoException lost
                && lost.getErrorCode().equals(REDISEARCH_CURSOR_REPLY_LOST.toErrorCode())) {
            // A failed-write replay filter completes the command with our exception. The synchronous
            // adapter wraps that exception too; retain the wrapper while restoring its external classification.
            return lostReply(cause);
        }
        return disconnected(cause) ? lostReply(cause) : cause;
    }

    private static TrinoException lostReply(Throwable cause) {
        return new TrinoException(REDISEARCH_CURSOR_REPLY_LOST,
                "A Query Engine cursor read could not be delivered reliably. It was not replayed because Redis "
                        + "may have consumed its batch. Verify server health and rerun the complete query.", cause);
    }

    private static RuntimeException driverCause(Throwable failure) {
        RuntimeException cause = RediSearchQueryErrors.classify(failure);
        // Lettuce's synchronous adapter wraps transport RedisExceptions in another RedisException.
        // Keep that wrapper as the reported cause, but recognize its disconnection for read failure
        // classification and bounded, idempotent deletion cleanup.
        while (cause.getClass() == RedisException.class && cause.getCause() instanceof RuntimeException nested) {
            cause = nested;
        }
        return cause;
    }

    private static boolean disconnected(Throwable failure) {
        RuntimeException cause = driverCause(failure);
        return cause instanceof RedisException && ("Connection disconnected".equals(cause.getMessage())
                || "Currently not connected. Commands are rejected.".equals(cause.getMessage()));
    }

    static CompletableFuture<String> delete(Supplier<CompletableFuture<String>> request) {
        return delete(request, 2);
    }

    private static CompletableFuture<String> delete(Supplier<CompletableFuture<String>> request, int retries) {
        CompletableFuture<String> sent;
        try {
            sent = request.get();
        } catch (RuntimeException failure) {
            sent = CompletableFuture.failedFuture(failure);
        }
        return sent.handle((reply, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(reply);
            }
            if (retries > 0 && disconnected(failure)) {
                // A deletion cannot consume result rows. Stay on the cursor's owning node and never wait
                // on a connection thread; after three failed attempts the caller logs the failure.
                return CompletableFuture.supplyAsync(() -> delete(request, retries - 1),
                        CompletableFuture.delayedExecutor(250, TimeUnit.MILLISECONDS)).thenCompose(Function.identity());
            }
            return CompletableFuture.<String>failedFuture(failure);
        }).thenCompose(Function.identity());
    }
}
