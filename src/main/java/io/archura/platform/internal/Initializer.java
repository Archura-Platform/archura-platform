package io.archura.platform.internal;

import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.FunctionIsNotAContextConsumerException;
import io.archura.platform.api.exception.FunctionIsNotAStreamConsumerException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.configuration.CacheConfiguration;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.IIFEConfiguration;
import io.archura.platform.internal.configuration.ScheduledConfiguration;
import io.archura.platform.internal.configuration.StreamConfiguration;
import io.archura.platform.internal.schedule.FunctionJob;
import io.archura.platform.internal.schedule.FunctionScheduler;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisBusyException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.RequiredArgsConstructor;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.RAMJobStore;
import org.quartz.spi.ThreadExecutor;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class Initializer {

    private final String configRepositoryUrl;
    private final HttpClient configurationHttpClient;
    private final ExecutorService executorService;
    private final Assets assets;
    private final FilterFunctionExecutor filterFunctionExecutor;


    public void initialize() {
        final GlobalConfiguration globalConfiguration = loadGlobalConfiguration();
        handleIIFEFunctions(globalConfiguration);
        handleStreamFunctions(globalConfiguration);
        handleScheduledFunctions(globalConfiguration);
    }

    private GlobalConfiguration loadGlobalConfiguration() {
        final GlobalConfiguration globalConfiguration = createGlobalConfiguration();
        GlobalConfiguration.setInstance(globalConfiguration);
        return globalConfiguration;
    }

    private GlobalConfiguration createGlobalConfiguration() {
        final String globalConfigURL = String.format("%s/imperative-shell/global/config.json", configRepositoryUrl);
        final GlobalConfiguration globalConfig = getGlobalConfiguration(globalConfigURL);
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
        cacheConfiguration.createRedisConnectionFactory();
        return cacheConfiguration;
    }

    private void handleIIFEFunctions(final GlobalConfiguration globalConfiguration) {
        final IIFEConfiguration iifeConfiguration = createIIFEConfiguration();
        globalConfiguration.setIifeConfiguration(iifeConfiguration);
        executeIIFEFunctions(globalConfiguration);
    }

    private IIFEConfiguration createIIFEConfiguration() {
        final String iffeConfigURL = String.format("%s/functional-core/iife/config.json", configRepositoryUrl);
        return getIIFEConfiguration(iffeConfigURL);
    }

    private IIFEConfiguration getIIFEConfiguration(String url) {
        return assets.getConfiguration(configurationHttpClient, url, IIFEConfiguration.class);
    }

    private void executeIIFEFunctions(GlobalConfiguration globalConfiguration) {
        // get redis commands
        final RedisCommands<String, String> redisCommands = globalConfiguration.getCacheConfiguration().getRedisCommands();
        // traverse configurations and execute functions
        final GlobalConfiguration.GlobalConfig globalConfig = globalConfiguration.getConfig();
        final String codeRepositoryUrl = globalConfig.getCodeRepositoryUrl();
        final IIFEConfiguration.Configuration iffeConfig = globalConfiguration.getIifeConfiguration().getConfig();
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
                        final String logLevel = getIIFELogLevel(globalConfig, iffeConfig, environmentConfig, tenantConfig, functionConfiguration);
                        final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, redisCommands);
                        // create function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final ContextConsumer contextConsumer = getIIFEFunction(codeRepositoryUrl, functionConfiguration, query);
                        // invoke function
                        executorService.submit(() -> filterFunctionExecutor.execute(context, contextConsumer));
                    } catch (Exception e) {
                        final String logLevel = getIIFELogLevel(globalConfig, iffeConfig, environmentConfig, tenantConfig, functionConfiguration);
                        final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, redisCommands);
                        context.getLogger().error("Error occurred while running IIFE function: %s - %s, error: %s", functionConfiguration.getName(), functionConfiguration.getVersion(), e.getMessage());
                    }
                }
            }
        }
    }

    private static String getIIFELogLevel(
            final GlobalConfiguration.Configuration globalConfig,
            final IIFEConfiguration.Configuration iffeConfig,
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
        if (nonNull(iffeConfig.getLogLevel())) {
            return iffeConfig.getLogLevel();
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
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (ContextConsumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final ContextConsumer contextConsumer = (ContextConsumer) object;
                return contextConsumer;
            } else {
                throw new FunctionIsNotAContextConsumerException(String.format("Resource is not a ContextConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private String getStreamConsumerLogLevel(
            final GlobalConfiguration.GlobalConfig globalConfig,
            final StreamConfiguration.Configuration streamConfig,
            final StreamConfiguration.Configuration environmentConfig,
            final StreamConfiguration.Configuration tenantConfig,
            final StreamConfiguration.ConsumerConfiguration consumerConfiguration
    ) {
        if (nonNull(consumerConfiguration.getLogLevel())) {
            return consumerConfiguration.getLogLevel();
        }
        if (nonNull(tenantConfig.getLogLevel())) {
            return tenantConfig.getLogLevel();
        }
        if (nonNull(environmentConfig.getLogLevel())) {
            return environmentConfig.getLogLevel();
        }
        if (nonNull(streamConfig.getLogLevel())) {
            return streamConfig.getLogLevel();
        }
        if (nonNull(globalConfig.getLogLevel())) {
            return globalConfig.getLogLevel();
        }
        return GlobalKeys.DEFAULT_LOG_LEVEL.getKey();
    }

    private Context createContextForEnvironmentAndTenant(
            final String environmentName,
            final String tenantId,
            final String logLevel,
            final RedisCommands<String, String> redisCommands
    ) {
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), environmentName);
        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), tenantId);
        if (nonNull(logLevel)) {
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
        }
        assets.buildContext(attributes, redisCommands);
        return (Context) attributes.get(Context.class.getSimpleName());
    }

    private void handleStreamFunctions(GlobalConfiguration globalConfiguration) {
        final StreamConfiguration streamConfiguration = createStreamConfiguration();
        globalConfiguration.setStreamConfiguration(streamConfiguration);
        executeStreamFunctions(globalConfiguration);
    }

    private StreamConfiguration createStreamConfiguration() {
        final String streamConfigURL = String.format("%s/functional-core/stream/config.json", configRepositoryUrl);
        return getStreamConfiguration(streamConfigURL);
    }

    private StreamConfiguration getStreamConfiguration(final String url) {
        return assets.getConfiguration(configurationHttpClient, url, StreamConfiguration.class);
    }

    private void executeStreamFunctions(final GlobalConfiguration globalConfiguration) {
        // get redis commands
        final RedisCommands<String, String> redisCommands = globalConfiguration.getCacheConfiguration().getRedisCommands();
        // traverse configurations and create subscriptions
        final GlobalConfiguration.GlobalConfig globalConfig = globalConfiguration.getConfig();
        final String codeRepositoryUrl = globalConfig.getCodeRepositoryUrl();
        final StreamConfiguration.Configuration streamConfig = globalConfiguration.getStreamConfiguration().getConfig();
        final Map<String, StreamConfiguration.EnvironmentConfiguration> environments = globalConfiguration.getStreamConfiguration().getEnvironments();
        // loop through environments
        for (Map.Entry<String, StreamConfiguration.EnvironmentConfiguration> environmentEntry : environments.entrySet()) {
            final String environmentName = environmentEntry.getKey();
            final StreamConfiguration.EnvironmentConfiguration environmentConfiguration = environmentEntry.getValue();
            final StreamConfiguration.Configuration environmentConfig = environmentConfiguration.getConfig();
            final Map<String, StreamConfiguration.TenantConfiguration> tenants = environmentConfiguration.getTenants();
            // loop through tenants
            for (Map.Entry<String, StreamConfiguration.TenantConfiguration> tenantEntry : tenants.entrySet()) {
                final String tenantId = tenantEntry.getKey();
                // read stream configuration
                final StreamConfiguration.TenantConfiguration tenantConfiguration = tenantEntry.getValue();
                final StreamConfiguration.Configuration tenantConfig = tenantConfiguration.getConfig();
                final List<StreamConfiguration.ConsumerConfiguration> consumers = tenantConfiguration.getConsumers();
                for (StreamConfiguration.ConsumerConfiguration consumerConfiguration : consumers) {
                    try {
                        // create context
                        final String logLevel = getStreamConsumerLogLevel(globalConfig, streamConfig, environmentConfig, tenantConfig, consumerConfiguration);
                        final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, redisCommands);
                        // create consumer function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final StreamConsumer streamConsumer = getStreamConsumerFunction(codeRepositoryUrl, consumerConfiguration, query);
                        // start/register stream function subscription
                        final String topic = consumerConfiguration.getTopic();
                        startStreamConsumerSubscription(environmentName, tenantId, topic, context, streamConsumer, redisCommands);
                    } catch (Exception e) {
                        // create context
                        final String logLevel = getStreamConsumerLogLevel(globalConfig, streamConfig, environmentConfig, tenantConfig, consumerConfiguration);
                        final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, redisCommands);
                        context.getLogger().error("Error occurred while subscribing Stream function: %s - %s, error: %s", consumerConfiguration.getName(), consumerConfiguration.getVersion(), e.getMessage());
                    }
                }
            }
        }
    }

    private StreamConsumer getStreamConsumerFunction(
            final String codeServerURL,
            final StreamConfiguration.ConsumerConfiguration configuration,
            final String query
    ) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (StreamConsumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final StreamConsumer handlerFunction = (StreamConsumer) object;
                return handlerFunction;
            } else {
                throw new FunctionIsNotAStreamConsumerException(String.format("Resource is not a StreamConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private void startStreamConsumerSubscription(
            final String environment,
            final String tenantId,
            final String topic,
            final Context context,
            final StreamConsumer streamConsumer,
            final RedisCommands<String, String> redisCommands
    ) {
        final Logger logger = context.getLogger();
        // CREATE STREAM AND GROUP FOR ENV-TENANT-TOPIC
        final String environmentTenantTopicName = String.format("%s|%s-%s", environment, tenantId, topic); // default|default-key1

//        final XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.latest(environmentTenantTopicName);
        final XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from(environmentTenantTopicName, "0-0");
        final XGroupCreateArgs xGroupCreateArgs = XGroupCreateArgs.Builder.mkstream();
        try {
            //final String result = redisCommands.xgroupCreate(streamOffset, environmentTenantTopicName, xGroupCreateArgs);
            final String result = redisCommands.xgroupCreate(streamOffset, environmentTenantTopicName, xGroupCreateArgs);
            logger.debug("Group '%s' created under topic '%s' with result: %s ", environmentTenantTopicName, environmentTenantTopicName, result);
        } catch (RedisBusyException exception) {
            logger.debug("Group '%s' already exists for topic '%s', message: %s", environmentTenantTopicName, environmentTenantTopicName, exception.getMessage());
        }
        executorService.execute(() -> {
            while (nonNull(redisCommands.clientId())) {
                try {
                    Thread.sleep(Duration.ofMillis(1000));
                    List<StreamMessage<String, String>> streamMessages = redisCommands.xreadgroup(
                            Consumer.from(environmentTenantTopicName, environmentTenantTopicName),
                            XReadArgs.StreamOffset.lastConsumed(environmentTenantTopicName)
                    );
                    if (nonNull(streamMessages) && !streamMessages.isEmpty()) {
                        logger.debug("Redis stream messages: '%s'", streamMessages);
                        streamMessages.forEach(message -> {
                            try {
                                filterFunctionExecutor.execute(context, streamConsumer, message.getId(), message.getBody());
                                redisCommands.xack(environmentTenantTopicName, environmentTenantTopicName, message.getId());
                                logger.info("Stream message acknowledged, id: '%s'", message.getId());
                            } catch (Exception exception) {
                                logger.error("Could not consume message: '%s'", message);
                            }
                        });
                    }
                } catch (RuntimeException | InterruptedException e) {
                    logger.error("Could not get message from stream: '%s', error: '%s'", environmentTenantTopicName, e.getMessage());
                }
            }
        });
    }

    private void handleScheduledFunctions(GlobalConfiguration globalConfiguration) {
        final ScheduledConfiguration scheduledConfiguration = createScheduledConfiguration();
        globalConfiguration.setScheduledConfiguration(scheduledConfiguration);
        try {
            executeScheduledFunctions(globalConfiguration);
        } catch (SchedulerException exception) {
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

    private void executeScheduledFunctions(final GlobalConfiguration globalConfiguration) throws SchedulerException {
        // create and start scheduler
        final QuartzSchedulerResources quartzSchedulerResources = new QuartzSchedulerResources();
        quartzSchedulerResources.setName("ScheduledFunctions");
        quartzSchedulerResources.setJobStore(new RAMJobStore());
        quartzSchedulerResources.setThreadExecutor(new ThreadExecutor() {
            @Override
            public void execute(Thread thread) {
                executorService.execute(thread);
            }

            @Override
            public void initialize() {
            }
        });
        final Scheduler scheduler = new StdScheduler(new FunctionScheduler(quartzSchedulerResources, 10_000, 10_000));
        scheduler.start();
        // get redis commands
        final RedisCommands<String, String> redisCommands = globalConfiguration.getCacheConfiguration().getRedisCommands();
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
                        final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, redisCommands);
                        // create consumer function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final ContextConsumer contextConsumer = getScheduledFunction(codeRepositoryUrl, scheduledFunctionConfiguration, query);
                        // schedule functions
                        scheduleFunction(scheduler, context, contextConsumer, scheduledFunctionConfiguration);
                    } catch (Exception e) {
                        // create context
                        final String logLevel = getScheduledFunctionLogLevel(globalConfig, scheduledConfig, environmentConfig, tenantConfig, scheduledFunctionConfiguration);
                        final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, redisCommands);
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
                @SuppressWarnings("unchecked") final ContextConsumer contextConsumer = (ContextConsumer) object;
                return contextConsumer;
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
            final Scheduler scheduler,
            final Context context,
            final ContextConsumer contextConsumer,
            final ScheduledConfiguration.FunctionConfiguration functionConfiguration
    ) throws SchedulerException {
        final Logger logger = context.getLogger();
        final String cron = functionConfiguration.getCron();
        final String scheduledFunctionName = contextConsumer.getClass().getName();
        if (nonNull(cron)) {
            final Map<Object, Object> objectMap = Map.of(
                    FilterFunctionExecutor.class.getSimpleName(), filterFunctionExecutor,
                    Context.class.getSimpleName(), context,
                    ContextConsumer.class.getSimpleName(), contextConsumer
            );
            final JobDetail jobDetail = JobBuilder.newJob(FunctionJob.class)
                    .withIdentity(scheduledFunctionName, scheduledFunctionName)
                    .setJobData(new JobDataMap(objectMap))
                    .build();
            final Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(scheduledFunctionName, scheduledFunctionName)
                    .forJob(jobDetail)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build();
            scheduler.scheduleJob(jobDetail, trigger);
            logger.debug("Scheduled function '%s' with cron '%s'", scheduledFunctionName, cron);
        } else {
            logger.error("Cron is not set for scheduled function '%s', will not schedule it.", scheduledFunctionName);
        }

    }

}
