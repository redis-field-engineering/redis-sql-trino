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

		configBinder(binder).bindConfig(RediSearchClientConfig.class);
	}

	@Singleton
	@Provides
	public static RediSearchSession createRediSearchSession(TypeManager typeManager, RediSearchClientConfig config) {
		requireNonNull(config, "config is null");
		Optional<String> uri = config.getUri();
		if (uri.isPresent()) {
			RedisModulesClient client = RedisModulesClient.create(uri.get());
			return new RediSearchSession(typeManager, client.connect(), config);
		}
		throw new IllegalArgumentException("No Redis URI specified");
	}
}
