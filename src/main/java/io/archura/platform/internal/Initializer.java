package io.archura.platform.internal;

import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.initialize.IIFEFunctionLoader;
import io.archura.platform.internal.initialize.GlobalConfigurationLoader;
import io.archura.platform.internal.initialize.ScheduledFunctionLoader;
import io.archura.platform.internal.initialize.StreamFunctionLoader;
import io.archura.platform.internal.initialize.SubscribedFunctionLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

@Slf4j
@RequiredArgsConstructor
public class Initializer {

    private final String configRepositoryUrl;
    private final HttpClient configurationHttpClient;
    private final ThreadFactory threadFactory;
    private final ExecutorService executorService;
    private final Assets assets;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public void initialize() {
        final GlobalConfigurationLoader globalConfigurationLoader = new GlobalConfigurationLoader(configRepositoryUrl, assets, configurationHttpClient, executorService, filterFunctionExecutor);
        final GlobalConfiguration globalConfiguration = globalConfigurationLoader.load();

        final IIFEFunctionLoader iifeFunctionLoader = new IIFEFunctionLoader(configRepositoryUrl, assets, configurationHttpClient, executorService, filterFunctionExecutor);
        iifeFunctionLoader.load(globalConfiguration);

        final StreamFunctionLoader streamFunctionLoader = new StreamFunctionLoader(configRepositoryUrl, assets, configurationHttpClient, executorService, filterFunctionExecutor);
        streamFunctionLoader.load(globalConfiguration);

        final SubscribedFunctionLoader subscribedFunctionLoader = new SubscribedFunctionLoader(configRepositoryUrl, assets, configurationHttpClient);
        subscribedFunctionLoader.load(globalConfiguration);

        ScheduledFunctionLoader scheduledFunctionLoader = new ScheduledFunctionLoader(configRepositoryUrl, assets, configurationHttpClient, threadFactory, filterFunctionExecutor);
        scheduledFunctionLoader.load(globalConfiguration);
    }

}
