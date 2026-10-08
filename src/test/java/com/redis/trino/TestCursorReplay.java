package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.search.AggregationReply.Cursor;
import io.trino.spi.TrinoException;

/** Exercises Lettuce's real reconnect path after a server consumes a command but drops its reply. */
class TestCursorReplay {
    @Test
    void nativeResp3WarningFailsBeforeRowsAndCursorIsDeletedWithoutReadRetry() throws Exception {
        try (LostReplyServer server = new LostReplyServer("FT.CURSOR", "READ", true);
                RedisClient client = server.client(ProtocolVersion.RESP3, true);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            var reply = connection.sync().ftCursorread("hits", Cursor.of(42, "node"), 1000);
            assertThat(reply.getReplies().get(0).getWarnings()).containsExactly("Timeout limit was reached");
            Throwable failure = catchThrowable(() -> RediSearchQueryErrors.verifyComplete(reply));
            assertThat(failure).isInstanceOf(TrinoException.class).hasMessageContaining("Timeout limit was reached");
            assertThat(((TrinoException) failure).getErrorCode())
                    .isEqualTo(RediSearchErrorCode.REDISEARCH_INCOMPLETE_RESULT.toErrorCode());
            assertThat(RediSearchCursorRecovery.delete(() -> connection.async()
                    .ftCursordel("hits", reply.getCursor().orElseThrow()).toCompletableFuture()).get(10, TimeUnit.SECONDS))
                    .isEqualTo("OK");
            assertThat(server.commands.get()).isEqualTo(1);
            assertThat(server.deletions.get()).isEqualTo(1);
        }
    }

    @Test
    void defaultDriverReplaysConsumedCursorRead() throws Exception {
        try (LostReplyServer server = new LostReplyServer("FT.CURSOR", "READ");
                RedisClient client = server.client(ProtocolVersion.RESP2, false);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            var read = connection.async().ftCursorread("hits", Cursor.of(42, "node"), 1000);
            read.get(10, TimeUnit.SECONDS);
            assertThat(server.commands.get()).isEqualTo(2);
            // Test the write-replay selector against native Lettuce commands, including its READ keyword encoding.
            var filter = RediSearchCursorRecovery.configure(ClientOptions.builder()).build().getReplayFilter();
            assertThat(filter.test((RedisCommand<?, ?, ?>) read)).isTrue();
            var deletion = connection.async().ftCursordel("hits", Cursor.of(42, "node"));
            deletion.get(10, TimeUnit.SECONDS);
            assertThat(filter.test((RedisCommand<?, ?, ?>) deletion)).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(value = ProtocolVersion.class, names = {"RESP2", "RESP3"})
    void failsLostCursorReplyWithoutReplayingButConnectionRecovers(ProtocolVersion protocol) throws Exception {
        try (LostReplyServer server = new LostReplyServer("FT.CURSOR", "READ");
                RedisClient client = server.client(protocol, true);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            var read = connection.async().ftCursorread("hits", Cursor.of(42, "node"), 1000).toCompletableFuture()
                    .exceptionally(failure -> { throw RediSearchCursorRecovery.readFailure(failure); });
            Throwable failure = catchThrowable(() -> read.get(10, TimeUnit.SECONDS));
            assertThat(failure).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TrinoException.class);
            assertThat(((TrinoException) failure.getCause()).getErrorCode())
                    .isEqualTo(RediSearchErrorCode.REDISEARCH_CURSOR_REPLY_LOST.toErrorCode());
            assertThat(RediSearchCursorRecovery.delete(() -> connection.async()
                    .ftCursordel("hits", Cursor.of(42, "node")).toCompletableFuture()).get(10, TimeUnit.SECONDS))
                    .isEqualTo("OK");
            assertThat(connection.sync().ping()).isEqualTo("PONG");
            assertThat(server.commands.get()).isEqualTo(1);
            assertThat(server.deletions.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ProtocolVersion.class, names = {"RESP2", "RESP3"})
    void synchronousLostCursorReplyPreservesTransportCauseAndFailsExternally(ProtocolVersion protocol) throws Exception {
        try (LostReplyServer server = new LostReplyServer("FT.CURSOR", "READ");
                RedisClient client = server.client(protocol, true);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            Throwable transport = catchThrowable(() -> connection.sync().ftCursorread("hits", Cursor.of(42, "node"), 1000));
            assertThat(transport).isInstanceOf(RedisException.class);
            RuntimeException failure = RediSearchCursorRecovery.readFailure(transport);
            assertThat(failure).isInstanceOf(TrinoException.class).hasCause(transport);
            assertThat(((TrinoException) failure).getErrorCode())
                    .isEqualTo(RediSearchErrorCode.REDISEARCH_CURSOR_REPLY_LOST.toErrorCode());
            assertThat(server.commands.get()).isEqualTo(1);
            assertThat(RediSearchCursorRecovery.delete(() -> connection.async()
                    .ftCursordel("hits", Cursor.of(42, "node")).toCompletableFuture()).get(10, TimeUnit.SECONDS))
                    .isEqualTo("OK");
            assertThat(server.deletions.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ProtocolVersion.class, names = {"RESP2", "RESP3"})
    void exactHashReadsWorkAgainAfterDisconnectedBatchFails(ProtocolVersion protocol) throws Exception {
        try (LostReplyServer server = new LostReplyServer("HGET", "doc");
                RedisClient client = server.client(protocol, true);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            var first = connection.async().hget("doc", "id");
            assertThat(catchThrowable(() -> first.get(10, TimeUnit.SECONDS)))
                    .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(RedisException.class);
            assertThat(server.reconnected.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(server.commands.get()).isEqualTo(1);
            assertThat(connection.async().hget("doc", "id").get(10, TimeUnit.SECONDS))
                    .isEqualTo("9007199254740993");
            assertThat(server.commands.get()).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ProtocolVersion.class, names = {"RESP2", "RESP3"})
    void cursorDeletionRetriesSafelyAfterLostReply(ProtocolVersion protocol) throws Exception {
        try (LostReplyServer server = new LostReplyServer("FT.CURSOR", "DEL");
                RedisClient client = server.client(protocol, true);
                StatefulRedisConnection<String, String> connection = client.connect()) {
            assertThat(RediSearchCursorRecovery.delete(() -> connection.async()
                    .ftCursordel("hits", Cursor.of(42, "node")).toCompletableFuture()).get(10, TimeUnit.SECONDS))
                    .isEqualTo("OK");
            assertThat(server.commands.get()).isEqualTo(2);
        }
    }

    @Test
    void deletionRetriesAreBoundedAndOtherServerErrorsArePreserved() {
        AtomicInteger attempts = new AtomicInteger();
        var disconnected = RediSearchCursorRecovery.delete(() -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedFuture(new RedisException("Connection disconnected"));
        });
        assertThat(catchThrowable(() -> disconnected.get(10, TimeUnit.SECONDS)))
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(RedisException.class);
        assertThat(attempts.get()).isEqualTo(3);
        attempts.set(0);
        var serverFailure = RediSearchCursorRecovery.delete(() -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedFuture(new RedisCommandExecutionException("Cursor not found"));
        });
        assertThat(catchThrowable(() -> serverFailure.get(10, TimeUnit.SECONDS)))
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(RedisCommandExecutionException.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    private static final class LostReplyServer implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
        private final ExecutorService executor = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().factory());
        private final AtomicInteger commands = new AtomicInteger();
        private final AtomicInteger deletions = new AtomicInteger();
        private final AtomicInteger activations = new AtomicInteger();
        private final CountDownLatch reconnected = new CountDownLatch(1);
        private final String type;
        private final String argument;
        private final boolean warning;
        private final Future<?> task;
        private volatile Socket active;
        private volatile boolean closed;

        LostReplyServer(String type, String argument) throws IOException {
            this(type, argument, false);
        }

        LostReplyServer(String type, String argument, boolean warning) throws IOException {
            this.type = type;
            this.argument = argument;
            this.warning = warning;
            task = executor.submit(() -> { serve(); return null; });
        }

        RedisClient client(ProtocolVersion protocol, boolean filter) {
            RedisClient client = RedisClient.create(RedisURI.Builder.redis("127.0.0.1", listener.getLocalPort())
                    .withTimeout(Duration.ofSeconds(5)).build());
            ClientOptions.Builder options = ClientOptions.builder().protocolVersion(protocol)
                    .pingBeforeActivateConnection(false).timeoutOptions(TimeoutOptions.enabled());
            if (filter) {
                RediSearchCursorRecovery.configure(options);
            }
            client.setOptions(options.build());
            client.addListener(new RedisConnectionStateListener() {
                @Override
                public void onRedisConnected(RedisChannelHandler<?, ?> connection) {
                    if (activations.incrementAndGet() > 1) {
                        reconnected.countDown();
                    }
                }
            });
            return client;
        }

        private void serve() throws IOException {
            while (!closed) {
                try (Socket socket = listener.accept()) {
                    active = socket;
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    while (!closed) {
                        List<String> command = read(input);
                        if (command.get(0).equals("FT.CURSOR") && command.get(1).equals("DEL")) {
                            deletions.incrementAndGet();
                        }
                        if (command.get(0).equals(type) && command.get(1).equals(argument)
                                && commands.incrementAndGet() == 1) {
                            if (warning) {
                                String reply = "*2\r\n%5\r\n+attributes\r\n*0\r\n+format\r\n+STRING\r\n"
                                        + "+results\r\n*0\r\n+total_results\r\n:0\r\n+warning\r\n*1\r\n"
                                        + "+Timeout limit was reached\r\n:42\r\n";
                                socket.getOutputStream().write(reply.getBytes(StandardCharsets.UTF_8));
                                socket.getOutputStream().flush();
                                continue;
                            }
                            // The cursor advanced (or the safe read/deletion executed), but no reply reached Lettuce.
                            break;
                        }
                        String reply = switch (command.get(0)) {
                            case "HELLO" -> "%7\r\n+server\r\n+redis\r\n+version\r\n+8.6.2\r\n+proto\r\n:3\r\n"
                                    + "+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n";
                            case "PING" -> "+PONG\r\n";
                            case "HGET" -> "$16\r\n9007199254740993\r\n";
                            case "FT.CURSOR" -> command.get(1).equals("READ")
                                    ? "*2\r\n*1\r\n:0\r\n:0\r\n" : "+OK\r\n";
                            case "CLIENT" -> "+OK\r\n";
                            default -> throw new IOException("Unexpected test command: " + command.get(0));
                        };
                        socket.getOutputStream().write(reply.getBytes(StandardCharsets.UTF_8));
                        socket.getOutputStream().flush();
                    }
                } catch (EOFException e) {
                    // A normal client close; accept the next connection until the fixture itself closes.
                } catch (SocketException e) {
                    if (!closed) { throw e; }
                }
            }
        }

        private static List<String> read(DataInputStream input) throws IOException {
            if (input.readByte() != '*') { throw new IOException("Expected RESP command array"); }
            int count = Integer.parseInt(line(input));
            List<String> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (input.readByte() != '$') { throw new IOException("Expected bulk command token"); }
                int size = Integer.parseInt(line(input));
                byte[] value = input.readNBytes(size);
                if (value.length != size || input.readByte() != '\r' || input.readByte() != '\n') {
                    throw new IOException("Truncated command token");
                }
                result.add(new String(value, StandardCharsets.UTF_8));
            }
            return result;
        }

        private static String line(DataInputStream input) throws IOException {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            for (byte value = input.readByte(); value != '\r'; value = input.readByte()) { result.write(value); }
            if (input.readByte() != '\n') { throw new IOException("Expected RESP line ending"); }
            return result.toString(StandardCharsets.US_ASCII);
        }

        @Override
        public void close() throws Exception {
            closed = true;
            listener.close();
            if (active != null) { active.close(); }
            executor.shutdown();
            task.get(5, TimeUnit.SECONDS);
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
