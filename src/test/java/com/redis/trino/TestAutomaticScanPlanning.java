package com.redis.trino;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.search.arguments.CreateArgs;
import io.lettuce.core.search.arguments.NumericFieldArgs;
import io.lettuce.core.search.arguments.SearchArgs;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DoubleType;

/** Exercises live MIN/MAX, range counts, skew rejection, caching and selective-scan fallback. */
public class TestAutomaticScanPlanning extends TestCursorReads {
    @Test
    public void testAutomaticDiscovery() {
        var redis = redisearch.getConnection().sync();
        redis.ftCreate("auto_scan", CreateArgs.builder().withPrefix("auto_scan:").build(), List.of(
                NumericFieldArgs.builder().name("constant").build(), NumericFieldArgs.builder().name("id").build()));
        for (int first = 0; first < 500_000; first += 1000) {
            redis.eval("for i=tonumber(ARGV[1]),tonumber(ARGV[1])+999 do "
                    + "redis.call('HSET','auto_scan:'..i,'id',i,'constant',0) end return 1000",
                    ScriptOutputType.INTEGER, new String[0], Integer.toString(first));
        }
        redisearch.awaitIndexed("auto_scan");
        var config = new RediSearchConfig().setUri(redisearch.getRedisURI());
        var session = new RediSearchSession(getDistributedQueryRunner().getCoordinator().getPlannerContext().getTypeManager(), config);
        try {
            var table = session.getTable(new SchemaTableName("default", "auto_scan"));
            var planner = new RediSearchAutoScanPlanner(session, () -> 8);
            var partitions = planner.plan(table.getTableHandle(), table.getIndexInfo());
            assertThat(partitions).hasSize(2).extracting(RediSearchScanPartition::field).containsOnly("id");
            assertThat(planner.plan(table.getTableHandle(), table.getIndexInfo())).isSameAs(partitions);
            long rows = partitions.stream().mapToLong(partition -> redis.ftSearch("auto_scan", partition.query(),
                    SearchArgs.<String>builder().limit(0, 0).build()).getCount()).sum();
            assertThat(rows).isEqualTo(500_000);
            var id = table.getColumns().stream().filter(column -> column.getName().equals("id")).findFirst().orElseThrow();
            var point = table.getTableHandle().withConstraint(TupleDomain.withColumnDomains(
                    Map.<ColumnHandle, Domain>of(id, Domain.singleValue(DoubleType.DOUBLE, 10.0))));
            assertThat(planner.plan(point, table.getIndexInfo())).isEmpty();
            assertThat(planner.plan(table.getTableHandle().withLimit(10), table.getIndexInfo())).isEmpty();
            config.setScanPartitionField("constant");
            assertThat(new RediSearchAutoScanPlanner(session, () -> 8)
                    .plan(table.getTableHandle(), table.getIndexInfo())).isEmpty();
        } finally {
            session.shutdown();
        }
    }
}
