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
				.setCaseInsensitiveNames(false)
				.setCursorCount(RediSearchConfig.DEFAULT_CURSOR_COUNT)
				.setTableCacheExpiration(RediSearchConfig.DEFAULT_TABLE_CACHE_EXPIRATION.toSeconds())
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
	public void testDefaultLimitIsDefunct() {
		ConfigurationFactory configurationFactory = new ConfigurationFactory(
				ImmutableMap.of("redisearch.default-limit", "10000"));
		assertThatThrownBy(() -> configurationFactory.build(RediSearchConfig.class))
				.hasMessageContaining("Defunct property 'redisearch.default-limit'");
	}

}
