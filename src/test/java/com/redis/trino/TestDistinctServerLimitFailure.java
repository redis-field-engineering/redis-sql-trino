package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.lettuce.core.RedisCommandExecutionException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

/** Protocol-level fault injection, not a claim of changing a real server's group budget. */
class TestDistinctServerLimitFailure {
    private static final String LIMIT_ERROR = "SEARCH_LIMIT_OVER Aggregate GROUPBY exceeded MAX_AGGREGATE_GROUPS limit of 2 groups";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stagedDistinctSurfacesServerLimitWithoutReturningOrReplayingOutput(boolean resp2) throws Exception {
        try (LimitServer server = new LimitServer()) {
            TypeManager unusedTypes = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                    TypeManager.class.getClassLoader(), new Class<?>[] {TypeManager.class},
                    (proxy, method, args) -> { throw new UnsupportedOperationException(method.getName()); });
            RediSearchSession session = new RediSearchSession(unusedTypes,
                    new RediSearchConfig().setUri(server.uri()).setResp2(resp2));
            try {
                RediSearchColumnHandle value = new RediSearchColumnHandle("v", VarcharType.VARCHAR,
                        RediSearchFieldType.TAG, false, true, Optional.empty());
                RediSearchColumnHandle count = new RediSearchColumnHandle("result", BigintType.BIGINT,
                        RediSearchFieldType.NUMERIC, false, false, Optional.empty());
                RediSearchTableHandle table = new RediSearchTableHandle(new SchemaTableName("default", "distinct_values"), "distinct_values")
                        .withAggregationStages(List.of(new RediSearchAggregationStage(
                                List.of(new RediSearchAggregationTerm("v", VarcharType.VARCHAR)),
                                List.of(new RediSearchAggregation("count", BigintType.BIGINT, Optional.empty(), "internal")), true)))
                        .appendAggregation(List.of(), List.of(new RediSearchAggregation("count", BigintType.BIGINT, Optional.of(value), "result")));
                // Construction must fail before a page source capable of emitting a partial count exists.
                assertThatThrownBy(() -> new RediSearchPageSource(session, table, List.of(count)))
                        .isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining(LIMIT_ERROR);
                List<List<String>> aggregates = server.commands.stream().filter(command -> command.getFirst().equals("FT.AGGREGATE")).toList();
                assertThat(aggregates).hasSize(1);
                assertThat(aggregates.getFirst().stream().filter("GROUPBY"::equals).count()).isEqualTo(2);
                assertThat(server.commands).noneMatch(command -> command.getFirst().equals("FT.CURSOR"));
            } finally {
                session.shutdown();
            }
        }
    }

    /** Same bounded RESP command parsing pattern as TestCursorReplay; only the failure response is synthetic. */
    private static final class LimitServer implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
        private final java.util.concurrent.ExecutorService executor = Executors.newCachedThreadPool(Thread.ofPlatform().daemon().factory());
        private final List<Socket> clients = new CopyOnWriteArrayList<>();
        private final List<List<String>> commands = new CopyOnWriteArrayList<>();
        private final List<Future<?>> handlers = new CopyOnWriteArrayList<>();
        private final Future<?> accept;
        private volatile boolean closed;

        LimitServer() throws IOException {
            accept = executor.submit(() -> {
                try {
                    while (!closed) {
                        Socket socket = listener.accept();
                        clients.add(socket);
                        handlers.add(executor.submit(() -> { serve(socket); return null; }));
                    }
                } catch (SocketException e) {
                    if (!closed) { throw e; }
                }
                return null;
            });
        }

        String uri() { return "redis://127.0.0.1:" + listener.getLocalPort() + "?timeout=5s"; }

        private void serve(Socket socket) throws IOException {
            try (socket; DataInputStream input = new DataInputStream(socket.getInputStream())) {
                while (!closed) {
                    List<String> command = read(input);
                    commands.add(command);
                    String reply = switch (command.getFirst()) {
                        case "HELLO" -> "%7\r\n+server\r\n+redis\r\n+version\r\n+8.8.0\r\n+proto\r\n:3\r\n"
                                + "+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n";
                        case "PING" -> "+PONG\r\n";
                        case "CLIENT" -> "+OK\r\n";
                        case "FT.INFO" -> "-Unknown index name\r\n";
                        case "FT.AGGREGATE" -> "-" + LIMIT_ERROR + "\r\n";
                        default -> throw new IOException("Unexpected test command: " + command.getFirst());
                    };
                    socket.getOutputStream().write(reply.getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                }
            } catch (EOFException e) {
                // Session shutdown closes its connections normally.
            } catch (SocketException e) {
                if (!closed) { throw e; }
            }
        }

        private static List<String> read(DataInputStream input) throws IOException {
            if (input.readByte() != '*') { throw new IOException("Expected RESP command array"); }
            int count = Integer.parseInt(line(input));
            if (count < 1 || count > 1000) { throw new IOException("Unexpected command length: " + count); }
            List<String> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (input.readByte() != '$') { throw new IOException("Expected bulk token"); }
                int size = Integer.parseInt(line(input));
                if (size < 0 || size > 100000) { throw new IOException("Unexpected token size: " + size); }
                byte[] value = input.readNBytes(size);
                if (value.length != size || input.readByte() != '\r' || input.readByte() != '\n') {
                    throw new IOException("Truncated command token");
                }
                result.add(new String(value, StandardCharsets.UTF_8));
            }
            return result;
        }

        private static String line(DataInputStream input) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            for (byte value = input.readByte(); value != '\r'; value = input.readByte()) {
                if (bytes.size() >= 20) { throw new IOException("RESP length too long"); }
                bytes.write(value);
            }
            if (input.readByte() != '\n') { throw new IOException("Expected line ending"); }
            return bytes.toString(StandardCharsets.US_ASCII);
        }

        @Override
        public void close() throws Exception {
            closed = true;
            listener.close();
            for (Socket socket : clients) { socket.close(); }
            accept.get(5, TimeUnit.SECONDS);
            for (Future<?> handler : handlers) { handler.get(5, TimeUnit.SECONDS); }
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
