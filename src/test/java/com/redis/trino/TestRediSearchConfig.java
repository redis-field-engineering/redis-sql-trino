package com.redis.trino;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.ConfigurationFactory;

public class TestRediSearchConfig {

	@Test
	public void testDefaults() {
		assertRecordedDefaults(recordDefaults(RediSearchConfig.class).setUri(null).setInsecure(false).setUsername(null)
				.setResp2(false).setPassword(null).setDefaultSchema(RediSearchConfig.DEFAULT_SCHEMA)
				.setDefaultLimit(RediSearchConfig.DEFAULT_LIMIT).setCaseInsensitiveNames(false)
				.setCursorCount(RediSearchConfig.DEFAULT_CURSOR_COUNT)
				.setTableCacheExpiration(RediSearchConfig.DEFAULT_TABLE_CACHE_EXPIRATION.toSeconds())
				.setTableCacheRefresh(RediSearchConfig.DEFAULT_TABLE_CACHE_REFRESH.toSeconds()).setCluster(false)
				.setCaCertPath(null).setKeyPassword(null).setKeyPath(null).setCertPath(null));
	}

	@Test
	public void testExplicitPropertyMappings() {
		String uri = "redis://redis.example.com:12000";
		String defaultSchema = "myschema";
		Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("redisearch.uri", uri)
				.put("redisearch.default-schema-name", defaultSchema).put("redisearch.resp2", "true").build();

		ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
		RediSearchConfig config = configurationFactory.build(RediSearchConfig.class);

		RediSearchConfig expected = new RediSearchConfig().setDefaultSchema(defaultSchema).setUri(uri);

		Assert.assertEquals(config.getDefaultSchema(), expected.getDefaultSchema());
		Assert.assertEquals(config.getUri(), expected.getUri());
		Assert.assertTrue(config.isResp2());
	}

}
