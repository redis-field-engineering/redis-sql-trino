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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

public class RediSearchConfig {

	public static final String DEFAULT_SCHEMA = "default";
	public static final long DEFAULT_LIMIT = 10000;
	public static final long DEFAULT_CURSOR_COUNT = 1000;
	public static final Duration DEFAULT_TABLE_CACHE_EXPIRATION = Duration.ofHours(1);
	public static final Duration DEFAULT_TABLE_CACHE_REFRESH = Duration.ofMinutes(1);

	private String defaultSchema = DEFAULT_SCHEMA;
	private Optional<String> uri = Optional.empty();
	private Optional<String> username = Optional.empty();
	private Optional<String> password = Optional.empty();
	private boolean insecure;
	private boolean caseInsensitiveNames;
	private long defaultLimit = DEFAULT_LIMIT;
	private long cursorCount = DEFAULT_CURSOR_COUNT;
	private long tableCacheExpiration = DEFAULT_TABLE_CACHE_EXPIRATION.toSeconds();
	private long tableCacheRefresh = DEFAULT_TABLE_CACHE_REFRESH.toSeconds();
	private boolean cluster;
	private String caCertPath;
	private String keyPath;
	private String certPath;
	private String keyPassword;
	private boolean resp2;

	@Min(0)
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

	public boolean isCaseInsensitiveNames() {
		return caseInsensitiveNames;
	}

	@Config("redisearch.case-insensitive-names")
	@ConfigDescription("Case-insensitive name-matching")
	public RediSearchConfig setCaseInsensitiveNames(boolean caseInsensitiveNames) {
		this.caseInsensitiveNames = caseInsensitiveNames;
		return this;
	}

	public boolean isResp2() {
		return resp2;
	}

	@Config("redisearch.resp2")
	@ConfigDescription("Force Redis protocol version to RESP2")
	public RediSearchConfig setResp2(boolean resp2) {
		this.resp2 = resp2;
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

	public boolean isCluster() {
		return cluster;
	}

	@Config("redisearch.cluster")
	@ConfigDescription("Connect to a Redis Cluster")
	public RediSearchConfig setCluster(boolean cluster) {
		this.cluster = cluster;
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

	public Optional<String> getCaCertPath() {
		return optionalPath(caCertPath);
	}

	private Optional<String> optionalPath(String path) {
		if (path == null || path.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(path);
	}

	@Config("redisearch.cacert-path")
	@ConfigDescription("X.509 CA certificate file to verify with")
	public RediSearchConfig setCaCertPath(String caCertPath) {
		this.caCertPath = caCertPath;
		return this;
	}

	public Optional<String> getKeyPath() {
		return optionalPath(keyPath);
	}

	@Config("redisearch.key-path")
	@ConfigDescription("PKCS#8 private key file to authenticate with (PEM format)")
	public RediSearchConfig setKeyPath(String keyPath) {
		this.keyPath = keyPath;
		return this;
	}

	public Optional<String> getKeyPassword() {
		return Optional.ofNullable(keyPassword);
	}

	@Config("redisearch.key-password")
	@ConfigSecuritySensitive
	@ConfigDescription("Password of the private key file, or null if it's not password-protected")
	public RediSearchConfig setKeyPassword(String keyPassword) {
		this.keyPassword = keyPassword;
		return this;
	}

	public Optional<String> getCertPath() {
		return optionalPath(certPath);
	}

	@Config("redisearch.cert-path")
	@ConfigDescription("X.509 certificate chain file to authenticate with (PEM format)")
	public RediSearchConfig setCertPath(String certPath) {
		this.certPath = certPath;
		return this;
	}

}
