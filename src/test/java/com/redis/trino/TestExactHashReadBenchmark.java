package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.redis.trino.RedisEnterprise.Deployment;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.trino.spi.type.TypeManager;

/** Manual command-throughput comparison; it does not time FT.AGGREGATE or SQL distinct computation. */
@EnabledIfSystemProperty(named = "redisearch.benchmark.exact-reads", matches = "true")
public class TestExactHashReadBenchmark {
    @Test
    public void benchmarkReads() {
        benchmark(Deployment.NON_SHARDED);
        benchmark(Deployment.SHARDED);
    }

    @SuppressWarnings("unchecked")
    private void benchmark(Deployment deployment) {
        try (RediSearchServer server = new RediSearchServer(deployment)) {
            try (var writes = server.getClient().connect()) {
                writes.setAutoFlushCommands(false);
                List<CompletableFuture<?>> pending = new ArrayList<>();
                for (int i = 1; i <= 10000; i++) {
                    pending.add(writes.async().hset("probe:" + i, "id", "9007199254740993").toCompletableFuture());
                }
                writes.flushCommands();
                CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).join();
            }
            // Hash commands don't look up SQL types. Fail if this benchmark starts doing so.
            TypeManager unusedTypes = (TypeManager) java.lang.reflect.Proxy.newProxyInstance(
                    TypeManager.class.getClassLoader(), new Class<?>[] { TypeManager.class },
                    (proxy, method, args) -> { throw new UnsupportedOperationException("No SQL type lookup in this benchmark"); });
            RediSearchSession session = new RediSearchSession(unusedTypes,
                    new RediSearchConfig().setUri(server.getRedisURI()).setCluster(deployment.isCluster()));
            try {
                RedisClusterAsyncCommands<String, String> commands = session.getConnection() instanceof StatefulRedisClusterConnection
                        ? ((StatefulRedisClusterConnection<String, String>)session.getConnection()).async()
                        : ((StatefulRedisConnection<String, String>)session.getConnection()).async();
                try (var buffered = session.exactHashReader()) {
                    for (int attempt = 0; attempt < 4; attempt++) {
                        for (boolean batch : attempt % 2 == 0 ? List.of(false, true) : List.of(true, false)) {
                            long start = System.nanoTime();
                            long received = 0;
                            for (int page = 0; page < 100; page++) {
                                List<CompletableFuture<String>> reads = new ArrayList<>();
                                for (int i = 0; i < 1000; i++) {
                                    String key = "probe:" + (1 + (page * 1000 + i) % 10000);
                                    reads.add(batch ? buffered.read(key, "id") : commands.hget(key, "id").toCompletableFuture());
                                }
                                if (batch) buffered.flush();
                                CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).join();
                                for (var read : reads) if ("9007199254740993".equals(read.join())) received++;
                            }
                            assertThat(received).isEqualTo(100000);
                            System.out.printf("READ_BENCH deployment=%s attempt=%d buffered=%s rows=%d millis=%.2f%n",
                                    deployment, attempt, batch, received, (System.nanoTime() - start) / 1e6);
                        }
                    }
                }
            } finally { session.shutdown(); }
        }
    }
}
