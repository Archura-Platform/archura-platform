package io.archura.platform.internal.initialize;

import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.FunctionIsNotAContextConsumerException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.IIFEConfiguration;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.stream.CacheStream;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.nonNull;

@Slf4j
public class IIFEFunctionLoader {

    private final String configRepositoryUrl;
    private final Assets assets;
    private final HttpClient configurationHttpClient;
    private final ExecutorService executorService;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public IIFEFunctionLoader(
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

    public void load(final GlobalConfiguration globalConfiguration) {
        try {
            final IIFEConfiguration iifeConfiguration = createIIFEConfiguration();
            globalConfiguration.setIifeConfiguration(iifeConfiguration);
            executeIIFEFunctions(globalConfiguration);
        } catch (Exception exception) {
            log.error("Error occurred while creating/running IIFE functions, error: {}", exception.getMessage());
        }
    }

    private IIFEConfiguration createIIFEConfiguration() {
        final String iifeConfigURL = String.format("%s/functional-core/iife/config.json", configRepositoryUrl);
        return getIIFEConfiguration(iifeConfigURL);
    }

    private IIFEConfiguration getIIFEConfiguration(String url) {
        return assets.getConfiguration(configurationHttpClient, url, IIFEConfiguration.class);
    }

    private void executeIIFEFunctions(GlobalConfiguration globalConfiguration) {
        // get commands
        final HashCache<String, String> hashCache = globalConfiguration.getCacheConfiguration().getHashCache();
        final CacheStream<String, Map<String, String>> cacheStream = globalConfiguration.getCacheConfiguration().getCacheStream();
        final MessagePublisher messagePublisher = globalConfiguration.getCacheConfiguration().getMessagePublisher();
        // traverse configurations and execute functions
        final GlobalConfiguration.GlobalConfig globalConfig = globalConfiguration.getConfig();
        final String codeRepositoryUrl = globalConfig.getCodeRepositoryUrl();
        final IIFEConfiguration.Configuration iifeConfig = globalConfiguration.getIifeConfiguration().getConfig();
        final Map<String, IIFEConfiguration.EnvironmentConfiguration> environments = globalConfiguration.getIifeConfiguration().getEnvironments();
        // loop through environments
        for (Map.Entry<String, IIFEConfiguration.EnvironmentConfiguration> environmentEntry : environments.entrySet()) {
            final String environmentName = environmentEntry.getKey();
            final IIFEConfiguration.EnvironmentConfiguration environmentConfiguration = environmentEntry.getValue();
            final IIFEConfiguration.Configuration environmentConfig = environmentConfiguration.getConfig();
            final Map<String, IIFEConfiguration.TenantConfiguration> tenants = environmentConfiguration.getTenants();
            // loop through tenants
            for (Map.Entry<String, IIFEConfiguration.TenantConfiguration> tenantEntry : tenants.entrySet()) {
                final String tenantId = tenantEntry.getKey();
                // read IIFE configuration
                final IIFEConfiguration.TenantConfiguration tenantConfiguration = tenantEntry.getValue();
                final IIFEConfiguration.Configuration tenantConfig = tenantConfiguration.getConfig();
                final List<IIFEConfiguration.FunctionConfiguration> functions = tenantConfiguration.getFunctions();
                for (IIFEConfiguration.FunctionConfiguration functionConfiguration : functions) {
                    try {
                        // create context
                        final String logLevel = getIIFELogLevel(globalConfig, iifeConfig, environmentConfig, tenantConfig, functionConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        // create function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final ContextConsumer contextConsumer = getIIFEFunction(codeRepositoryUrl, functionConfiguration, query);
                        // invoke function
                        executorService.submit(() -> filterFunctionExecutor.execute(context, contextConsumer));
                    } catch (Exception e) {
                        final String logLevel = getIIFELogLevel(globalConfig, iifeConfig, environmentConfig, tenantConfig, functionConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        context.getLogger().error("Error occurred while running IIFE function: %s - %s, error: %s", functionConfiguration.getName(), functionConfiguration.getVersion(), e.getMessage());
                    }
                }
            }
        }
    }

    private static String getIIFELogLevel(
            final GlobalConfiguration.Configuration globalConfig,
            final IIFEConfiguration.Configuration iifeConfig,
            final IIFEConfiguration.Configuration environmentConfig,
            final IIFEConfiguration.Configuration tenantConfig,
            final IIFEConfiguration.FunctionConfiguration functionConfiguration
    ) {
        if (nonNull(functionConfiguration.getLogLevel())) {
            return functionConfiguration.getLogLevel();
        }
        if (nonNull(tenantConfig.getLogLevel())) {
            return tenantConfig.getLogLevel();
        }
        if (nonNull(environmentConfig.getLogLevel())) {
            return environmentConfig.getLogLevel();
        }
        if (nonNull(iifeConfig.getLogLevel())) {
            return iifeConfig.getLogLevel();
        }
        if (nonNull(globalConfig.getLogLevel())) {
            return globalConfig.getLogLevel();
        }
        return GlobalKeys.DEFAULT_LOG_LEVEL.getKey();
    }

    private ContextConsumer getIIFEFunction(String codeServerURL, IIFEConfiguration.FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceKey, configuration.getName(), configuration.getConfig());
            if (ContextConsumer.class.isAssignableFrom(object.getClass())) {
                return (ContextConsumer) object;
            } else {
                throw new FunctionIsNotAContextConsumerException(String.format("Resource is not a ContextConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

}
