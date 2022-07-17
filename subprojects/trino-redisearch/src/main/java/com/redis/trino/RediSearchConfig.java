package com.redis.trino;

import java.time.Duration;
import java.util.Optional;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class RediSearchConfig {

	public static final String DEFAULT_SCHEMA = "default";
	public static final long DEFAULT_LIMIT = 10000;
	public static final Duration DEFAULT_TABLE_CACHE_EXPIRATION = Duration.ofHours(1);
	public static final Duration DEFAULT_TABLE_CACHE_REFRESH = Duration.ofMinutes(1);

	private String defaultSchema = DEFAULT_SCHEMA;
	private Optional<String> uri = Optional.empty();
	private boolean caseInsensitiveNameMatching;
	private long defaultLimit = DEFAULT_LIMIT;
	private long cursorCount = 0; // Use RediSearch default
	private long tableCacheExpiration = DEFAULT_TABLE_CACHE_EXPIRATION.toSeconds();
	private long tableCacheRefresh = DEFAULT_TABLE_CACHE_REFRESH.toSeconds();

	public long getCursorCount() {
		return cursorCount;
	}

	@Config("redisearch.cursor-count")
	public RediSearchConfig setCursorCount(long cursorCount) {
		this.cursorCount = cursorCount;
		return this;
	}

	public long getDefaultLimit() {
		return defaultLimit;
	}

	@Config("redisearch.default-limit")
	@ConfigDescription("Default search limit number to use")
	public RediSearchConfig setDefaultLimit(long defaultLimit) {
		this.defaultLimit = defaultLimit;
		return this;
	}

	@Config("redisearch.table-cache-expiration")
	@ConfigDescription("Duration in seconds since the entry creation after which a table should be automatically removed from the cache.")
	public RediSearchConfig setTableCacheExpiration(long expirationDuration) {
		this.tableCacheExpiration = expirationDuration;
		return this;
	}

	public long getTableCacheExpiration() {
		return tableCacheExpiration;
	}

	@Config("redisearch.table-cache-refresh")
	@ConfigDescription("Duration in seconds since the entry creation after which to automatically refresh the table cache.")
	public RediSearchConfig setTableCacheRefresh(long refreshDuration) {
		this.tableCacheRefresh = refreshDuration;
		return this;
	}

	public long getTableCacheRefresh() {
		return tableCacheRefresh;
	}

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

	public boolean isCaseInsensitiveNameMatching() {
		return caseInsensitiveNameMatching;
	}

	@Config("redisearch.case-insensitive-name-matching")
	@ConfigDescription("Case-insensitive name-matching")
	public RediSearchConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching) {
		this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
		return this;
	}

}
