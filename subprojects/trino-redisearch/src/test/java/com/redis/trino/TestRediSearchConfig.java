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
		assertRecordedDefaults(
				recordDefaults(RediSearchConfig.class).setUri(null).setDefaultSchema(RediSearchConfig.DEFAULT_SCHEMA)
						.setDefaultLimit(RediSearchConfig.DEFAULT_LIMIT).setCaseInsensitiveNameMatching(false));
	}

	@Test
	public void testExplicitPropertyMappings() {
		String uri = "redis://redis.example.com:12000";
		String defaultSchema = "myschema";
		Map<String, String> properties = new ImmutableMap.Builder<String, String>().put("redisearch.uri", uri)
				.put("redisearch.default-schema-name", defaultSchema).build();

		ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
		RediSearchConfig config = configurationFactory.build(RediSearchConfig.class);

		RediSearchConfig expected = new RediSearchConfig().setDefaultSchema(defaultSchema).setUri(uri);

		assertEquals(config.getDefaultSchema(), expected.getDefaultSchema());
		assertEquals(config.getUri(), expected.getUri());
	}

}
