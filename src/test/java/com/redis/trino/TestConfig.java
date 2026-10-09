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
				.setCursorCount(RediSearchConfig.DEFAULT_CURSOR_COUNT)
				.setQueryTimeoutMillis(0)
				.setScanConnections(RediSearchConfig.DEFAULT_SCAN_CONNECTIONS)
                .setScanSplits(0).setScanPartitionField(null).setScanPartitionBoundaries(java.util.List.of())
				.setTableCacheRefresh(RediSearchConfig.DEFAULT_TABLE_CACHE_REFRESH.toSeconds()).setCluster(false)
				.setCaCertPath(null).setKeyPassword(null).setKeyPath(null).setCertPath(null)
				.setNarrowScanCursorCount(0).setAggregationGroupLimit(1000000).setAggregationPushdownEnabled(true)
				.setDynamicFilteringEnabled(true)
				.setDynamicFilteringWaitTimeout(RediSearchConfig.DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT));
	}

	@Test
	public void testExplicitPropertyMappings() {
		String uri = "redis://redis.example.com:12000";
		String defaultSchema = "myschema";
		Map<String, String> properties = ImmutableMap.<String, String>builder().put("redisearch.uri", uri)
				.put("redisearch.default-schema-name", defaultSchema).put("redisearch.resp2", "true")
				.put("redisearch.aggregation-pushdown.enabled", "false")
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
		assertThat(config.getScanConnections()).isEqualTo(2);
        assertThat(config.getScanSplits()).isEqualTo(8);
        assertThat(config.getScanPartitionField()).isEqualTo("id");
        assertThat(config.getScanPartitionBoundaries()).containsExactly(0.0, 10.0, 100.0);
		assertThat(config.getQueryTimeoutMillis()).isEqualTo(1200000);
		assertThat(config.getNarrowScanCursorCount()).isEqualTo(10000);
		assertThat(config.getAggregationGroupLimit()).isEqualTo(100);
		assertThat(config.isAggregationPushdownEnabled()).isFalse();
		assertThat(config.isDynamicFilteringEnabled()).isFalse();
		assertThat(config.getDynamicFilteringWaitTimeout()).isEqualTo(new Duration(3, TimeUnit.SECONDS));
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

}
