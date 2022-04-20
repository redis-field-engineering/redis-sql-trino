package com.redis.trino;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.ConfigurationFactory;

public class TestRediSearchConfig {

	@Test
	public void testDefaults() {
		assertRecordedDefaults(recordDefaults(RediSearchClientConfig.class).setUri(null)
				.setDefaultSchema(RediSearchClientConfig.DEFAULT_SCHEMA)
				.setDefaultLimit(RediSearchClientConfig.DEFAULT_LIMIT).setCaseInsensitiveNameMatching(false)
				.setCursorCount(0));
	}

	@Test
	public void testExplicitPropertyMappings() {
		String uri = "redis://redis.example.com:12000";
		String defaultSchema = "myschema";
		Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("redisearch.uri", uri)
				.put("redisearch.default-schema-name", defaultSchema).build();

		ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
		RediSearchClientConfig config = configurationFactory.build(RediSearchClientConfig.class);

		RediSearchClientConfig expected = new RediSearchClientConfig().setDefaultSchema(defaultSchema).setUri(uri);

		assertEquals(config.getDefaultSchema(), expected.getDefaultSchema());
		assertEquals(config.getUri(), expected.getUri());
	}

}
