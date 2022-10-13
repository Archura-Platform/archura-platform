package io.archura.platform.internal.initialize;

import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.FunctionIsNotAContextConsumerException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.ScheduledConfiguration;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.stream.CacheStream;

import java.net.http.HttpClient;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static java.util.Objects.nonNull;

public class ScheduledFunctionLoader {
    private final String configRepositoryUrl;
    private final Assets assets;
    private final HttpClient configurationHttpClient;
    private final ThreadFactory threadFactory;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public ScheduledFunctionLoader(
            final String configRepositoryUrl,
            final Assets assets,
            final HttpClient configurationHttpClient,
            final ThreadFactory threadFactory,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        this.configRepositoryUrl = configRepositoryUrl;
        this.assets = assets;
        this.configurationHttpClient = configurationHttpClient;
        this.threadFactory = threadFactory;
        this.filterFunctionExecutor = filterFunctionExecutor;
    }

    public void load(GlobalConfiguration globalConfiguration) {
        final ScheduledConfiguration scheduledConfiguration = createScheduledConfiguration();
        globalConfiguration.setScheduledConfiguration(scheduledConfiguration);
        try {
            executeScheduledFunctions(globalConfiguration);
        } catch (Exception exception) {
            final Logger logger = assets.getLogger(Collections.emptyMap());
            logger.error("Could not schedule function, error: '%s'", exception.getMessage());
        }
    }

    private ScheduledConfiguration createScheduledConfiguration() {
        final String streamConfigURL = String.format("%s/functional-core/scheduled/config.json", configRepositoryUrl);
        return getScheduledConfiguration(streamConfigURL);
    }

    private ScheduledConfiguration getScheduledConfiguration(final String url) {
        return assets.getConfiguration(configurationHttpClient, url, ScheduledConfiguration.class);
    }

    private void executeScheduledFunctions(final GlobalConfiguration globalConfiguration) {
        // get commands
        final HashCache<String, String> hashCache = globalConfiguration.getCacheConfiguration().getHashCache();
        final CacheStream<String, Map<String, String>> cacheStream = globalConfiguration.getCacheConfiguration().getCacheStream();
        final MessagePublisher messagePublisher = globalConfiguration.getCacheConfiguration().getMessagePublisher();
        // traverse configurations and create schedules
        final GlobalConfiguration.GlobalConfig globalConfig = globalConfiguration.getConfig();
        final String codeRepositoryUrl = globalConfig.getCodeRepositoryUrl();
        final ScheduledConfiguration.Configuration scheduledConfig = globalConfiguration.getScheduledConfiguration().getConfig();
        final Map<String, ScheduledConfiguration.EnvironmentConfiguration> environments = globalConfiguration.getScheduledConfiguration().getEnvironments();
        // loop through environments
        for (Map.Entry<String, ScheduledConfiguration.EnvironmentConfiguration> environmentEntry : environments.entrySet()) {
            final String environmentName = environmentEntry.getKey();
            final ScheduledConfiguration.EnvironmentConfiguration environmentConfiguration = environmentEntry.getValue();
            final ScheduledConfiguration.Configuration environmentConfig = environmentConfiguration.getConfig();
            final Map<String, ScheduledConfiguration.TenantConfiguration> tenants = environmentConfiguration.getTenants();
            // loop through tenants
            for (Map.Entry<String, ScheduledConfiguration.TenantConfiguration> tenantEntry : tenants.entrySet()) {
                final String tenantId = tenantEntry.getKey();
                // read Scheduled configuration
                final ScheduledConfiguration.TenantConfiguration tenantConfiguration = tenantEntry.getValue();
                final ScheduledConfiguration.Configuration tenantConfig = tenantConfiguration.getConfig();
                final List<ScheduledConfiguration.FunctionConfiguration> functions = tenantConfiguration.getFunctions();
                for (ScheduledConfiguration.FunctionConfiguration scheduledFunctionConfiguration : functions) {
                    try {
                        // create context
                        final String logLevel = getScheduledFunctionLogLevel(globalConfig, scheduledConfig, environmentConfig, tenantConfig, scheduledFunctionConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        // create consumer function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final ContextConsumer contextConsumer = getScheduledFunction(codeRepositoryUrl, scheduledFunctionConfiguration, query);
                        // schedule functions
                        scheduleFunction(context, contextConsumer, scheduledFunctionConfiguration, threadFactory);
                    } catch (Exception e) {
                        // create context
                        final String logLevel = getScheduledFunctionLogLevel(globalConfig, scheduledConfig, environmentConfig, tenantConfig, scheduledFunctionConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        context.getLogger().error("Error occurred while scheduling scheduled function: %s - %s, error: %s", scheduledFunctionConfiguration.getName(), scheduledFunctionConfiguration.getVersion(), e.getMessage());
                    }
                }
            }
        }
    }

    private ContextConsumer getScheduledFunction(
            final String codeServerURL,
            final ScheduledConfiguration.FunctionConfiguration configuration,
            final String query
    ) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (ContextConsumer.class.isAssignableFrom(object.getClass())) {
                return (ContextConsumer) object;
            } else {
                throw new FunctionIsNotAContextConsumerException(String.format("Resource is not a ContextConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private String getScheduledFunctionLogLevel(
            final GlobalConfiguration.GlobalConfig globalConfig,
            final ScheduledConfiguration.Configuration scheduledConfig,
            final ScheduledConfiguration.Configuration environmentConfig,
            final ScheduledConfiguration.Configuration tenantConfig,
            final ScheduledConfiguration.FunctionConfiguration scheduledFunctionConfiguration
    ) {
        if (nonNull(scheduledFunctionConfiguration.getLogLevel())) {
            return scheduledFunctionConfiguration.getLogLevel();
        }
        if (nonNull(tenantConfig.getLogLevel())) {
            return tenantConfig.getLogLevel();
        }
        if (nonNull(environmentConfig.getLogLevel())) {
            return environmentConfig.getLogLevel();
        }
        if (nonNull(scheduledConfig.getLogLevel())) {
            return scheduledConfig.getLogLevel();
        }
        if (nonNull(globalConfig.getLogLevel())) {
            return globalConfig.getLogLevel();
        }
        return GlobalKeys.DEFAULT_LOG_LEVEL.getKey();
    }

    private void scheduleFunction(
            final Context context,
            final ContextConsumer contextConsumer,
            final ScheduledConfiguration.FunctionConfiguration functionConfiguration,
            final ThreadFactory threadFactory
    ) {
        final Logger logger = context.getLogger();
        final long delay = functionConfiguration.getDelay();
        final String timeUnitValue = functionConfiguration.getTimeUnit();
        final TimeUnit timeUnit = TimeUnit.valueOf(timeUnitValue);
        final String scheduledFunctionName = contextConsumer.getClass().getName();
        if (delay > 0) {
            try {
                final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1_000, threadFactory);
                scheduledExecutorService.scheduleWithFixedDelay(() -> filterFunctionExecutor.execute(context, contextConsumer), 1, delay, timeUnit);
                logger.debug("Scheduled function '%s' with delay: '%s' and timeUnit: '%s'", scheduledFunctionName, delay, timeUnit);
            } catch (Exception exception) {
                logger.error("Got exception while scheduling function '%s', error: %s.", scheduledFunctionName, exception.getMessage());
            }
        } else {
            logger.error("Delay cannot be zero or less for scheduled function '%s', will not schedule it.", scheduledFunctionName);
        }
    }

}
