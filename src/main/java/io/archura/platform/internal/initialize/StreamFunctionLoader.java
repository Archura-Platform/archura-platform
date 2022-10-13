package io.archura.platform.internal.initialize;

import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.FunctionIsNotAStreamConsumerException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.StreamConfiguration;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.stream.CacheStream;
import io.lettuce.core.*;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.nonNull;

public class StreamFunctionLoader {

    private final String configRepositoryUrl;
    private final Assets assets;
    private final HttpClient configurationHttpClient;
    private final ExecutorService executorService;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public StreamFunctionLoader(
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

    public void load(GlobalConfiguration globalConfiguration) {
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
        // get commands
        final HashCache<String, String> hashCache = globalConfiguration.getCacheConfiguration().getHashCache();
        final CacheStream<String, Map<String, String>> cacheStream = globalConfiguration.getCacheConfiguration().getCacheStream();
        final MessagePublisher messagePublisher = globalConfiguration.getCacheConfiguration().getMessagePublisher();

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
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        // create consumer function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final StreamConsumer streamConsumer = getStreamConsumerFunction(codeRepositoryUrl, consumerConfiguration, query);
                        // start/register stream function subscription
                        final String topic = consumerConfiguration.getTopic();
                        startStreamConsumerSubscription(environmentName, tenantId, topic, context, streamConsumer, cacheStream);
                    } catch (Exception e) {
                        // create context
                        final String logLevel = getStreamConsumerLogLevel(globalConfig, streamConfig, environmentConfig, tenantConfig, consumerConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
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
                return (StreamConsumer) object;
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
            final CacheStream<String, Map<String, String>> cacheStream
    ) {
        final Logger logger = context.getLogger();
        // CREATE STREAM AND GROUP FOR ENV-TENANT-TOPIC
        final String environmentTenantTopicName = String.format("stream|%s|%s|%s", environment, tenantId, topic);
        final String environmentTenantTopicGroup = String.format("group|%s|%s|%s|%s", environment, tenantId, topic, streamConsumer.getClass().getSimpleName());

        final XReadArgs.StreamOffset<String> streamOffset = XReadArgs.StreamOffset.from(environmentTenantTopicName, "0-0");
        final XGroupCreateArgs xGroupCreateArgs = XGroupCreateArgs.Builder.mkstream();
        try {
            final String result = cacheStream.createGroup(streamOffset, environmentTenantTopicGroup, xGroupCreateArgs);
            logger.debug("Group '%s' created under topic '%s' with result: %s ", environmentTenantTopicGroup, environmentTenantTopicName, result);
        } catch (RedisBusyException exception) {
            logger.debug("Group '%s' already exists for topic '%s', message: %s", environmentTenantTopicGroup, environmentTenantTopicName, exception.getMessage());
        }
        executorService.execute(() -> {
            while (true) {
                try {
                    Thread.sleep(Duration.ofMillis(1000));
                    final List<StreamMessage<String, String>> streamMessages = cacheStream.readMessageFromGroup(
                            Consumer.from(environmentTenantTopicGroup, environmentTenantTopicGroup),
                            XReadArgs.StreamOffset.lastConsumed(environmentTenantTopicName)
                    );
                    if (nonNull(streamMessages) && !streamMessages.isEmpty()) {
                        logger.debug("Redis stream messages: '%s'", streamMessages);
                        streamMessages.forEach(message -> {
                            try {
                                filterFunctionExecutor.execute(context, streamConsumer, message.getId(), message.getBody());
                                cacheStream.acknowledge(environmentTenantTopicName, environmentTenantTopicGroup, message.getId());
                                logger.info("Stream message acknowledged, id: '%s'", message.getId());
                            } catch (Exception exception) {
                                logger.error("Could not consume message: '%s', error: '%s'", message, exception.getMessage());
                            }
                        });
                    }
                } catch (RuntimeException | InterruptedException e) {
                    logger.error("Could not get message from stream: '%s', error: '%s'", environmentTenantTopicName, e.getMessage());
                }
            }
        });
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

}
