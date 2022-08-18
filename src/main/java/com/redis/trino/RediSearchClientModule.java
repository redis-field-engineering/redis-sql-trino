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

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

import java.util.Optional;

import javax.inject.Singleton;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.redis.lettucemod.RedisModulesClient;

import io.trino.spi.type.TypeManager;

public class RediSearchClientModule implements Module {

	@Override
	public void configure(Binder binder) {
		binder.bind(RediSearchConnector.class).in(Scopes.SINGLETON);
		binder.bind(RediSearchSplitManager.class).in(Scopes.SINGLETON);
		binder.bind(RediSearchPageSourceProvider.class).in(Scopes.SINGLETON);
		binder.bind(RediSearchPageSinkProvider.class).in(Scopes.SINGLETON);

		configBinder(binder).bindConfig(RediSearchConfig.class);
	}

	@Singleton
	@Provides
	public static RediSearchSession createRediSearchSession(TypeManager typeManager, RediSearchConfig config) {
		requireNonNull(config, "config is null");
		Optional<String> uri = config.getUri();
		if (uri.isPresent()) {
			RedisModulesClient client = RedisModulesClient.create(uri.get());
			return new RediSearchSession(typeManager, client.connect(), config);
		}
		throw new IllegalArgumentException("No Redis URI specified");
	}
}
