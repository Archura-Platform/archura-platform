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
import io.archura.platform.internal.configuration.CacheConfiguration;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.IIFEConfiguration;
import io.archura.platform.internal.configuration.ScheduledConfiguration;
import io.archura.platform.internal.configuration.StreamConfiguration;
import io.archura.platform.internal.stream.RedisStreamSubscription;
import io.lettuce.core.RedisBusyException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.CronTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class Initializer implements SchedulingConfigurer {

    private final String configRepositoryUrl;
    private final HttpClient configurationHttpClient;
    private final ConfigurableBeanFactory beanFactory;
    private final ThreadFactory threadFactory;
    private final ExecutorService executorService;
    private final Assets assets;
    private final RedisStreamSubscription redisStreamSubscription = new RedisStreamSubscription();
    private ScheduledTaskRegistrar scheduledTaskRegistrar;

    public void initialize() {
        final GlobalConfiguration globalConfiguration = loadGlobalConfiguration(beanFactory);
        handleIIFEFunctions(globalConfiguration);
        handleStreamFunctions(globalConfiguration);
        handleScheduledFunctions(globalConfiguration);
    }

    private GlobalConfiguration loadGlobalConfiguration(ConfigurableBeanFactory beanFactory) {
        final GlobalConfiguration globalConfiguration = createGlobalConfiguration();
        registerGlobalConfigurationBean(beanFactory, globalConfiguration);
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
        cacheConfiguration.createHashOperations();
        cacheConfiguration.createStreamOperations();
        return cacheConfiguration;
    }

    private void registerGlobalConfigurationBean(ConfigurableBeanFactory beanFactory, GlobalConfiguration globalConfiguration) {
        final String beanName = GlobalConfiguration.class.getSimpleName();
        try {
            beanFactory.isSingleton(beanName);
            final DefaultListableBeanFactory factory = (DefaultListableBeanFactory) beanFactory;
            factory.destroySingleton(beanName);
            beanFactory.registerSingleton(beanName, globalConfiguration);
        } catch (NoSuchBeanDefinitionException e) {
            beanFactory.registerSingleton(beanName, globalConfiguration);
        }
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
        // get hash and stream operation objects
        final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
        final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();
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
                    // create function
                    final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                    final ContextConsumer contextConsumer = getIIFEFunction(codeRepositoryUrl, functionConfiguration, query);
                    // create context
                    final String logLevel = getIIFELogLevel(globalConfig, iffeConfig, environmentConfig, tenantConfig, functionConfiguration);
                    final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashOperations, streamOperations);
                    // invoke function
                    executorService.submit(() -> contextConsumer.accept(context));
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
            final String tenantId,
            final String environmentName,
            final String logLevel,
            final HashOperations<String, String, Map<String, Object>> hashOperations,
            final StreamOperations<String, Object, Object> streamOperations
    ) {
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), environmentName);
        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), tenantId);
        if (nonNull(logLevel)) {
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
        }
        assets.buildContext(attributes, hashOperations, streamOperations);
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
        // get hash and stream operation objects
        final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
        final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();
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
                // read IIFE configuration
                final StreamConfiguration.TenantConfiguration tenantConfiguration = tenantEntry.getValue();
                final StreamConfiguration.Configuration tenantConfig = tenantConfiguration.getConfig();
                final List<StreamConfiguration.ConsumerConfiguration> consumers = tenantConfiguration.getConsumers();
                for (StreamConfiguration.ConsumerConfiguration consumerConfiguration : consumers) {
                    // create consumer function
                    final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                    final StreamConsumer streamConsumer = getStreamConsumerFunction(codeRepositoryUrl, consumerConfiguration, query);
                    // create context
                    final String logLevel = getStreamConsumerLogLevel(globalConfig, streamConfig, environmentConfig, tenantConfig, consumerConfiguration);
                    final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashOperations, streamOperations);
                    // start/register stream function subscription
                    final String topic = consumerConfiguration.getTopic();
                    startStreamConsumerSubscription(environmentName, tenantId, topic, context, streamConsumer, globalConfiguration);
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
            final GlobalConfiguration globalConfiguration
    ) {
        final Logger logger = context.getLogger();
        // CREATE STREAM AND GROUP FOR ENV-TENANT-TOPIC
        final String environmentTenantTopicName = String.format("%s|%s-%s", environment, tenantId, topic); // default|default-key1
        final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();
        try {
            final String groupCreationResult = streamOperations.createGroup(environmentTenantTopicName, environmentTenantTopicName);
            logger.debug("Group '%s' created under topic '%s' with result: %s ", environmentTenantTopicName, environmentTenantTopicName, groupCreationResult);
        } catch (RedisSystemException e) {
            if (e.getCause() instanceof RedisBusyException redisBusyException) {
                logger.debug("Redis BUSY exception occurred while creating group '%s', error: %s", environmentTenantTopicName, redisBusyException.getMessage());
            } else {
                logger.error("Exception occurred while creating group '%s', error: '%s'", environmentTenantTopicName, e.getMessage());
            }
        }
        // CREATE REDIS BEAN
        final StreamListener<String, ObjectRecord<String, byte[]>> redisStreamListener =
                message -> {
                    final byte[] key = message.getId().getValue().getBytes(StandardCharsets.UTF_8);
                    final byte[] value = message.getValue();
                    streamConsumer.consume(context, key, value);
                };
        final LettuceConnectionFactory redisConnectionFactory = globalConfiguration.getCacheConfiguration().getRedisConnectionFactory();
        final Subscription subscription = redisStreamSubscription.createConsumerSubscription(
                redisConnectionFactory,
                redisStreamListener,
                environmentTenantTopicName,
                executorService
        );
        // CREATE BEAN
        final String streamConsumerBeanName = String.format("%s-%s", environmentTenantTopicName, streamConsumer.hashCode()); // default|default-key1-00110011
        try {
            beanFactory.isSingleton(streamConsumerBeanName);
            logger.debug("Stream consumer bean with id '%s' already exists, will remove the bean and register new bean.", streamConsumerBeanName);
            final DefaultListableBeanFactory factory = (DefaultListableBeanFactory) beanFactory;
            factory.destroySingleton(streamConsumerBeanName);
            beanFactory.registerSingleton(streamConsumerBeanName, subscription);
        } catch (NoSuchBeanDefinitionException e) {
            beanFactory.registerSingleton(streamConsumerBeanName, subscription);
        }
        final Object streamConsumerBean = beanFactory.getBean(streamConsumerBeanName);
        logger.debug("Stream consumer created with id '%s', bean: '%s'", streamConsumerBeanName, streamConsumerBean);
    }

    private void handleScheduledFunctions(GlobalConfiguration globalConfiguration) {
        final ScheduledConfiguration scheduledConfiguration = createScheduledConfiguration();
        globalConfiguration.setScheduledConfiguration(scheduledConfiguration);
        executeScheduledFunctions(globalConfiguration);
    }

    private ScheduledConfiguration createScheduledConfiguration() {
        final String streamConfigURL = String.format("%s/functional-core/scheduled/config.json", configRepositoryUrl);
        return getScheduledConfiguration(streamConfigURL);
    }

    private ScheduledConfiguration getScheduledConfiguration(final String url) {
        return assets.getConfiguration(configurationHttpClient, url, ScheduledConfiguration.class);
    }

    private void executeScheduledFunctions(final GlobalConfiguration globalConfiguration) {
        // get hash and stream operation objects
        final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
        final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();
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
                    // create consumer function
                    final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                    final ContextConsumer contextConsumer = getScheduledFunction(codeRepositoryUrl, scheduledFunctionConfiguration, query);
                    // create context
                    final String logLevel = getScheduledFunctionLogLevel(globalConfig, scheduledConfig, environmentConfig, tenantConfig, scheduledFunctionConfiguration);
                    final Context context = createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashOperations, streamOperations);
                    // schedule functions
                    scheduleFunction(context, contextConsumer, scheduledFunctionConfiguration);
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
            final Context context,
            final ContextConsumer contextConsumer,
            final ScheduledConfiguration.FunctionConfiguration functionConfiguration
    ) {
        final Logger logger = context.getLogger();
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10_000, threadFactory);
        scheduledTaskRegistrar.setScheduler(executor);
        final String cron = functionConfiguration.getCron();
        final String zone = functionConfiguration.getZone();
        final String scheduledFunctionName = contextConsumer.getClass().getName();
        if (nonNull(cron)) {
            final TimeZone timeZone = Optional.ofNullable(zone)
                    .map(TimeZone::getTimeZone)
                    .orElse(TimeZone.getTimeZone(ZoneOffset.UTC));
            final CronTrigger cronTrigger = new CronTrigger(cron, timeZone);
            final CronTask cronTask = new CronTask(() -> contextConsumer.accept(context), cronTrigger);
            scheduledTaskRegistrar.scheduleCronTask(cronTask);
            logger.debug("Scheduled function '%s' with cron '%s' and time zone '%s'", scheduledFunctionName, cron, timeZone.getDisplayName());
        } else {
            logger.error("Cron is not set for scheduled function '%s', will not schedule it.", scheduledFunctionName);
        }
    }

    @Override
    public void configureTasks(final ScheduledTaskRegistrar taskRegistrar) {
        this.scheduledTaskRegistrar = taskRegistrar;
    }
}
