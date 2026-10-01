package com.redis.trino;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.ConfigurationFactory;

public class TestConfig {

	@Test
	public void testDefaults() {
		assertRecordedDefaults(recordDefaults(RediSearchConfig.class).setUri(null).setInsecure(false).setUsername(null)
				.setResp2(false).setPassword(null).setDefaultSchema(RediSearchConfig.DEFAULT_SCHEMA)
				.setDefaultLimit(RediSearchConfig.DEFAULT_LIMIT).setCaseInsensitiveNames(false)
				.setCursorCount(RediSearchConfig.DEFAULT_CURSOR_COUNT)
				.setTableCacheRefresh(RediSearchConfig.DEFAULT_TABLE_CACHE_REFRESH.toSeconds()).setCluster(false)
				.setCaCertPath(null).setKeyPassword(null).setKeyPath(null).setCertPath(null));
	}

	@Test
	public void testExplicitPropertyMappings() {
		String uri = "redis://redis.example.com:12000";
		String defaultSchema = "myschema";
		Map<String, String> properties = ImmutableMap.<String, String>builder().put("redisearch.uri", uri)
				.put("redisearch.default-schema-name", defaultSchema).put("redisearch.resp2", "true").buildOrThrow();

		ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
		RediSearchConfig config = configurationFactory.build(RediSearchConfig.class);

		RediSearchConfig expected = new RediSearchConfig().setDefaultSchema(defaultSchema).setUri(uri);

		assertThat(config.getDefaultSchema()).isEqualTo(expected.getDefaultSchema());
		assertThat(config.getUri()).isEqualTo(expected.getUri());
		assertThat(config.isResp2()).isTrue();
	}

	@Test
	public void testDefunctTableCacheExpiration() {
		Map<String, String> properties = ImmutableMap.of("redisearch.uri", "redis://redis.example.com:12000",
				"redisearch.table-cache-expiration", "3600");

		ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);

		assertThatThrownBy(() -> configurationFactory.build(RediSearchConfig.class))
				.hasMessageContaining("Defunct property 'redisearch.table-cache-expiration'");
	}

}
