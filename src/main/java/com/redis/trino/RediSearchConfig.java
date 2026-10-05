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
import java.util.concurrent.TimeUnit;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;

// redisearch.default-limit capped every scan without a SQL LIMIT, silently truncating the rows Trino aggregated and
// joined over. redisearch.table-cache-expiration was never read; the table cache only uses
// redisearch.table-cache-refresh
@DefunctConfig({ "redisearch.default-limit", "redisearch.table-cache-expiration" })
public class RediSearchConfig {

    public static final String DEFAULT_SCHEMA = "default";

    public static final long DEFAULT_CURSOR_COUNT = 1000;

    public static final Duration DEFAULT_TABLE_CACHE_REFRESH = Duration.ofMinutes(1);

    // As long as Trino's JDBC connectors wait by default
    public static final io.airlift.units.Duration DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT = new io.airlift.units.Duration(
            20, TimeUnit.SECONDS);

    private String defaultSchema = DEFAULT_SCHEMA;

    private String uri;

    private String username;

    private String password;

    private boolean insecure;

    private boolean cluster;

    private String caCertPath;

    private String keyPath;

    private String certPath;

    private String keyPassword;

    private boolean resp2;

    private boolean caseInsensitiveNames;

    private long cursorCount = DEFAULT_CURSOR_COUNT;

    private long tableCacheRefresh = DEFAULT_TABLE_CACHE_REFRESH.toSeconds();
    private boolean dynamicFilteringEnabled = true;
    private io.airlift.units.Duration dynamicFilteringWaitTimeout = DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT;

    @Min(0)
    public long getCursorCount() {
        return cursorCount;
    }

    @Config("redisearch.cursor-count")
    public RediSearchConfig setCursorCount(long cursorCount) {
        this.cursorCount = cursorCount;
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

    @Config("redisearch.table-cache-refresh")
    @ConfigDescription("Duration in seconds since the entry creation after which to automatically refresh the table cache.")
    public RediSearchConfig setTableCacheRefresh(long refreshDuration) {
        this.tableCacheRefresh = refreshDuration;
        return this;
    }

    public long getTableCacheRefresh() {
        return tableCacheRefresh;
    }

    public boolean isDynamicFilteringEnabled() {
        return dynamicFilteringEnabled;
    }

    @Config("redisearch.dynamic-filtering.enabled")
    @ConfigDescription("Add the join keys a join's build side collects to the query that scans the other side")
    public RediSearchConfig setDynamicFilteringEnabled(boolean dynamicFilteringEnabled) {
        this.dynamicFilteringEnabled = dynamicFilteringEnabled;
        return this;
    }

    @NotNull
    public io.airlift.units.Duration getDynamicFilteringWaitTimeout() {
        return dynamicFilteringWaitTimeout;
    }

    @Config("redisearch.dynamic-filtering.wait-timeout")
    @ConfigDescription("How long a scan waits for the build side of a join to collect its dynamic filters")
    public RediSearchConfig setDynamicFilteringWaitTimeout(io.airlift.units.Duration dynamicFilteringWaitTimeout) {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
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
    public @Pattern(message = "Invalid Redis URI. Expected redis:// rediss://", regexp = "^rediss?://.*") String getUri() {
        return uri;
    }

    @Config("redisearch.uri")
    @ConfigDescription("Redis connection URI e.g. 'redis://localhost:6379'")
    @ConfigSecuritySensitive
    public RediSearchConfig setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getUsername() {
        return username;
    }

    @Config("redisearch.username")
    @ConfigDescription("Redis connection username")
    @ConfigSecuritySensitive
    public RediSearchConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    @Config("redisearch.password")
    @ConfigDescription("Redis connection password")
    @ConfigSecuritySensitive
    public RediSearchConfig setPassword(String password) {
        this.password = password;
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

    public String getCaCertPath() {
        return caCertPath;
    }

    @Config("redisearch.cacert-path")
    @ConfigDescription("X.509 CA certificate file to verify with")
    public RediSearchConfig setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
        return this;
    }

    public String getKeyPath() {
        return keyPath;
    }

    @Config("redisearch.key-path")
    @ConfigDescription("PKCS#8 private key file to authenticate with (PEM format)")
    public RediSearchConfig setKeyPath(String keyPath) {
        this.keyPath = keyPath;
        return this;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    @Config("redisearch.key-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password of the private key file, or null if it's not password-protected")
    public RediSearchConfig setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public String getCertPath() {
        return certPath;
    }

    @Config("redisearch.cert-path")
    @ConfigDescription("X.509 certificate chain file to authenticate with (PEM format)")
    public RediSearchConfig setCertPath(String certPath) {
        this.certPath = certPath;
        return this;
    }

}
