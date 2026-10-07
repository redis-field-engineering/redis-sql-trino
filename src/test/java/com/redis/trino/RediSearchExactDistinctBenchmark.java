package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import com.redis.trino.RedisEnterprise.Deployment;

import io.trino.operator.OperatorStats;
import io.trino.spi.metrics.Metrics;

/** Manual end-to-end Q5 comparison on 100,000 exact, distinct large integers. */
public class RediSearchExactDistinctBenchmark {
    @Test
    public void benchmark() throws Exception {
        try (RediSearchServer server = new RediSearchServer(Deployment.SHARDED);
                var runner = RediSearchQueryRunner.createRediSearchQueryRunner(server, List.of(), Map.of(),
                        Map.of("redisearch.cursor-count", "1000", "redisearch.scan-connections", "4"))) {
            runner.execute("CREATE TABLE hits (userid bigint, marker varchar)");
            try (var writes = server.getClient().connect()) {
                writes.setAutoFlushCommands(false);
                for (int batch = 0; batch < 100; batch++) {
                    List<CompletableFuture<?>> pending = new ArrayList<>();
                    for (int i = 0; i < 1000; i++) {
                        int row = batch * 1000 + i;
                        pending.add(writes.async().hset("hits:" + row,
                                Map.of("userid", String.valueOf(9007199254740992L + row), "marker", "row")).toCompletableFuture());
                    }
                    writes.flushCommands();
                    CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).join();
                }
            }
            for (int attempt = 0; attempt < 4; attempt++) {
                long start = System.nanoTime();
                var result = runner.executeWithPlan(runner.getDefaultSession(), "SELECT COUNT(DISTINCT UserID) FROM hits");
                double millis = (System.nanoTime() - start) / 1e6;
                assertThat(result.result().getOnlyValue()).isEqualTo(100000L);
                Metrics metrics = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId())
                        .getQueryStats().getOperatorSummaries().stream().map(OperatorStats::getConnectorMetrics)
                        .reduce(Metrics.EMPTY, Metrics::mergeWith);
                System.out.println(String.format("Q5_BENCH label=%s attempt=%d millis=%.2f result=%s metrics=%s",
                        System.getProperty("benchmark.label", "candidate"), attempt, millis,
                        result.result().getOnlyValue(), metrics.getMetrics()));
            }
        }
    }
}
