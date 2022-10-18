package io.archura.platform.internal.initialize;

import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.configuration.CacheConfiguration;
import io.archura.platform.internal.configuration.GlobalConfiguration;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;

public class GlobalConfigurationLoader {

    private final String configRepositoryUrl;
    private final Assets assets;
    private final HttpClient configurationHttpClient;
    private final ExecutorService executorService;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public GlobalConfigurationLoader(
            final String configRepositoryUrl,
            final Assets assets,
            final HttpClient configurationHttpClient,
            final ExecutorService executorService,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        this.configRepositoryUrl = configRepositoryUrl;
        this.assets = assets;
        this.configurationHttpClient = configurationHttpClient;
        this.executorService = executorService;
        this.filterFunctionExecutor = filterFunctionExecutor;
    }

    public GlobalConfiguration load() {
        final GlobalConfiguration globalConfiguration = createGlobalConfiguration();
        GlobalConfiguration.setInstance(globalConfiguration);
        return globalConfiguration;
    }

    private GlobalConfiguration createGlobalConfiguration() {
        final String globalConfigURL = String.format("%s/global/config.json", configRepositoryUrl);
        final GlobalConfiguration globalConfig = getGlobalConfiguration(globalConfigURL);
        final String globalImperativeConfigURL = String.format("%s/imperative-shell/global/config.json", configRepositoryUrl);
        final GlobalConfiguration globalFiltersConfig = getGlobalConfiguration(globalImperativeConfigURL);
        globalConfig.setPost(globalFiltersConfig.getPost());
        globalConfig.setPre(globalFiltersConfig.getPre());
        final String redisUrl = globalConfig.getConfig().getRedisUrl();
        final CacheConfiguration cacheConfiguration = createCacheConfiguration(redisUrl);
        globalConfig.setCacheConfiguration(cacheConfiguration);
        return globalConfig;
    }

    private GlobalConfiguration getGlobalConfiguration(String url) {
        return assets.getConfiguration(configurationHttpClient, url, GlobalConfiguration.class);
    }

    private CacheConfiguration createCacheConfiguration(final String redisUrl) {
        final CacheConfiguration cacheConfiguration = new CacheConfiguration(redisUrl);
        cacheConfiguration.createConnections(executorService, filterFunctionExecutor);
        return cacheConfiguration;
    }

}
