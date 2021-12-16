package com.redis.trino;

import java.util.Optional;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class RediSearchConfig {

	public static final String DEFAULT_SCHEMA = "default";
	
	private String defaultSchema = DEFAULT_SCHEMA;
	private Optional<String> uri = Optional.empty();

	@NotNull
	public String getDefaultSchema() {
		return defaultSchema;
	}

	@Config("redisearch.default-schema-name")
	@ConfigDescription("Default schema name to use")
	public RediSearchConfig setDefaultSchema(String defaultSchema) {
		this.defaultSchema = defaultSchema;
		return this;
	}

	@NotNull
	public Optional<@Pattern(message = "Invalid Redis URI. Expected redis:// rediss://", regexp = "^rediss?://.*") String> getUri() {
		return uri;
	}

	@Config("redisearch.uri")
	@ConfigDescription("Redis connection URI e.g. 'redis://localhost:6379'")
	@ConfigSecuritySensitive
	public RediSearchConfig setUri(String uri) {
		this.uri = Optional.ofNullable(uri);
		return this;
	}

}
