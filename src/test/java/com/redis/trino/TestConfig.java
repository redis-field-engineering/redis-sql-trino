package com.redis.trino;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;

public class TestConfig {

	@Test
	public void testDefaults() {
		assertRecordedDefaults(recordDefaults(RediSearchConfig.class).setUri(null).setInsecure(false).setUsername(null)
				.setResp2(false).setPassword(null).setDefaultSchema(RediSearchConfig.DEFAULT_SCHEMA)
				.setCaseInsensitiveNames(false)
				.setFlexEnabled(false).setFlexBatchSize(1000)
				.setCursorCount(RediSearchConfig.DEFAULT_CURSOR_COUNT)
				.setQueryTimeoutMillis(0)
				.setScanConnections(RediSearchConfig.DEFAULT_SCAN_CONNECTIONS)
                .setScanSplits(0).setScanPartitionField(null).setScanPartitionBoundaries(java.util.List.of())
				.setTableCacheRefresh(RediSearchConfig.DEFAULT_TABLE_CACHE_REFRESH.toSeconds()).setCluster(false)
				.setCaCertPath(null).setKeyPassword(null).setKeyPath(null).setCertPath(null)
				.setNarrowScanCursorCount(0).setAggregationGroupLimit(1000000).setAggregationPushdownEnabled(true)
				.setDynamicFilteringEnabled(true).setAtomicHashUpdatesEnabled(false).setLocalTopNMaxCandidates(1000)
				.setDynamicFilteringWaitTimeout(RediSearchConfig.DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT));
	}

	@Test
	public void testExplicitPropertyMappings() {
		String uri = "redis://redis.example.com:12000";
		String defaultSchema = "myschema";
		Map<String, String> properties = ImmutableMap.<String, String>builder().put("redisearch.uri", uri)
				.put("redisearch.default-schema-name", defaultSchema).put("redisearch.resp2", "true")
				.put("redisearch.aggregation-pushdown.enabled", "false")
                .put("redisearch.atomic-hash-updates.enabled", "true")
                .put("redisearch.local-topn.max-candidates", "2000")
				.put("redisearch.flex.enabled", "true").put("redisearch.flex.batch-size", "23")
				.put("redisearch.query-timeout-ms", "1200000")
				.put("redisearch.narrow-scan-cursor-count", "10000").put("redisearch.aggregation-group-limit", "100")
				.put("redisearch.scan-splits", "8").put("redisearch.scan-partition-field", "id")
                .put("redisearch.scan-partition-boundaries", "0,10,100")
                .put("redisearch.scan-connections", "2").put("redisearch.dynamic-filtering.enabled", "false").put("redisearch.dynamic-filtering.wait-timeout", "3s")
				.buildOrThrow();

		ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
		RediSearchConfig config = configurationFactory.build(RediSearchConfig.class);

		RediSearchConfig expected = new RediSearchConfig().setDefaultSchema(defaultSchema).setUri(uri);

		assertThat(config.getDefaultSchema()).isEqualTo(expected.getDefaultSchema());
		assertThat(config.getUri()).isEqualTo(expected.getUri());
		assertThat(config.isResp2()).isTrue();
		assertThat(config.isFlexEnabled()).isTrue();
		assertThat(config.getFlexBatchSize()).isEqualTo(23);
		assertThat(config.getScanConnections()).isEqualTo(2);
        assertThat(config.getScanSplits()).isEqualTo(8);
        assertThat(config.getScanPartitionField()).isEqualTo("id");
        assertThat(config.getScanPartitionBoundaries()).containsExactly(0.0, 10.0, 100.0);
		assertThat(config.getQueryTimeoutMillis()).isEqualTo(1200000);
		assertThat(config.getNarrowScanCursorCount()).isEqualTo(10000);
		assertThat(config.getAggregationGroupLimit()).isEqualTo(100);
		assertThat(config.isAggregationPushdownEnabled()).isFalse();
        assertThat(config.isAtomicHashUpdatesEnabled()).isTrue();
        assertThat(config.getLocalTopNMaxCandidates()).isEqualTo(2000);
		assertThat(config.isDynamicFilteringEnabled()).isFalse();
		assertThat(config.getDynamicFilteringWaitTimeout()).isEqualTo(new Duration(3, TimeUnit.SECONDS));
	}

	@Test
	public void testFlexBatchBounds() {
		for (String size : java.util.List.of("0", "10001")) {
			assertThatThrownBy(() -> new ConfigurationFactory(Map.of("redisearch.uri", "redis://localhost:6379",
					"redisearch.flex.batch-size", size)).build(RediSearchConfig.class))
					.isInstanceOf(RuntimeException.class).hasMessageContaining("redisearch.flex.batch-size");
		}
		for (String size : java.util.List.of("1", "10000")) {
			assertThat(new ConfigurationFactory(Map.of("redisearch.uri", "redis://localhost:6379",
					"redisearch.flex.batch-size", size)).build(RediSearchConfig.class).getFlexBatchSize())
					.isEqualTo(Integer.parseInt(size));
		}
	}

	@Test
	public void testDefaultLimitIsDefunct() {
		ConfigurationFactory configurationFactory = new ConfigurationFactory(
				ImmutableMap.of("redisearch.default-limit", "10000"));
		assertThatThrownBy(() -> configurationFactory.build(RediSearchConfig.class))
				.hasMessageContaining("Defunct property 'redisearch.default-limit'");
	}

	@Test
	public void testTableCacheExpirationIsDefunct() {
		ConfigurationFactory configurationFactory = new ConfigurationFactory(
				ImmutableMap.of("redisearch.table-cache-expiration", "3600"));
		assertThatThrownBy(() -> configurationFactory.build(RediSearchConfig.class))
				.hasMessageContaining("Defunct property 'redisearch.table-cache-expiration'");
	}

    @Test
    public void testLocalTopNBounds() {
        for (String value : java.util.List.of("-1", "100001")) {
            assertThatThrownBy(() -> new ConfigurationFactory(Map.of("redisearch.uri", "redis://localhost:6379",
                    "redisearch.local-topn.max-candidates", value)).build(RediSearchConfig.class))
                    .isInstanceOf(RuntimeException.class).hasMessageContaining("redisearch.local-topn.max-candidates");
        }
        for (String value : java.util.List.of("0", "100000")) {
            assertThat(new ConfigurationFactory(Map.of("redisearch.uri", "redis://localhost:6379",
                    "redisearch.local-topn.max-candidates", value)).build(RediSearchConfig.class).getLocalTopNMaxCandidates())
                    .isEqualTo(Integer.parseInt(value));
        }
    }

}
