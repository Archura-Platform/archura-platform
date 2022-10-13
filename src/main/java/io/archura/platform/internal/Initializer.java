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
public class Initializer {

    private final GlobalConfigurationLoader globalConfigurationLoader;
    private final IIFEFunctionLoader iifeFunctionLoader;
    private final StreamFunctionLoader streamFunctionLoader;
    private final SubscribedFunctionLoader subscribedFunctionLoader;
    private final ScheduledFunctionLoader scheduledFunctionLoader;

    public Initializer(
            final String configRepositoryUrl,
            final HttpClient configurationHttpClient,
            final ThreadFactory threadFactory,
            final ExecutorService executorService,
            final Assets assets,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        globalConfigurationLoader = new GlobalConfigurationLoader(configRepositoryUrl, assets, configurationHttpClient, executorService, filterFunctionExecutor);
        iifeFunctionLoader = new IIFEFunctionLoader(configRepositoryUrl, assets, configurationHttpClient, executorService, filterFunctionExecutor);
        streamFunctionLoader = new StreamFunctionLoader(configRepositoryUrl, assets, configurationHttpClient, executorService, filterFunctionExecutor);
        subscribedFunctionLoader = new SubscribedFunctionLoader(configRepositoryUrl, assets, configurationHttpClient);
        scheduledFunctionLoader = new ScheduledFunctionLoader(configRepositoryUrl, assets, configurationHttpClient, threadFactory, filterFunctionExecutor);
    }

    public void initialize() {
        final GlobalConfiguration globalConfiguration = globalConfigurationLoader.load();
        iifeFunctionLoader.load(globalConfiguration);
        streamFunctionLoader.load(globalConfiguration);
        subscribedFunctionLoader.load(globalConfiguration);
        scheduledFunctionLoader.load(globalConfiguration);
    }

}
