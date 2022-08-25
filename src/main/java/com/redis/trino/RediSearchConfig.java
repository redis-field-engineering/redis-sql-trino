/*
 * MIT License
 *
 * Copyright (c) 2022, Redis Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
	private Optional<String> username = Optional.empty();
	private Optional<String> password = Optional.empty();
	private boolean caseInsensitiveNameMatching;
	private boolean insecure;
	private boolean tls;
	private long timeout;
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

	public Optional<String> getUsername() {
		return username;
	}

	@Config("redisearch.username")
	@ConfigDescription("Redis connection username")
	@ConfigSecuritySensitive
	public RediSearchConfig setUsername(String username) {
		this.username = Optional.ofNullable(username);
		return this;
	}

	public Optional<String> getPassword() {
		return password;
	}

	@Config("redisearch.password")
	@ConfigDescription("Redis connection password")
	@ConfigSecuritySensitive
	public RediSearchConfig setPassword(String password) {
		this.password = Optional.ofNullable(password);
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

	public boolean isInsecure() {
		return insecure;
	}

	@Config("redisearch.insecure")
	@ConfigDescription("Allow insecure connections (e.g. invalid certificates) to Redis when using SSL")
	public RediSearchConfig setInsecure(boolean insecure) {
		this.insecure = insecure;
		return this;
	}

	public boolean isTls() {
		return tls;
	}

	@Config("redisearch.tls")
	@ConfigDescription("Establish a secure TLS connection")
	public RediSearchConfig setTls(boolean tls) {
		this.tls = tls;
		return this;
	}

	public long getTimeout() {
		return timeout;
	}

	@Config("redisearch.timeout")
	@ConfigDescription("Redis command timeout in seconds")
	public RediSearchConfig setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}
}
