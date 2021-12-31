package com.redis.trino;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

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
		if (config.getUri().isPresent()) {
			RedisModulesClient client = RedisModulesClient.create(config.getUri().get());
			return new RediSearchSession(typeManager, client.connect(), config);
		}
		throw new IllegalArgumentException("No Redis URI specified");
	}
}