package io.archura.platform.internal.initialize;

import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.FunctionIsNotASubscriptionConsumerException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.functionalcore.SubscriptionConsumer;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.SubscribedConfiguration;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.pubsub.PublishListener;
import io.archura.platform.internal.pubsub.Subscriber;
import io.archura.platform.internal.stream.CacheStream;
import io.lettuce.core.RedisException;

import java.net.http.HttpClient;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

public class SubscribedFunctionLoader {
    private final String configRepositoryUrl;
    private final Assets assets;
    private final HttpClient configurationHttpClient;

    public SubscribedFunctionLoader(
            final String configRepositoryUrl,
            final Assets assets,
            final HttpClient configurationHttpClient
    ) {
        this.configRepositoryUrl = configRepositoryUrl;
        this.assets = assets;
        this.configurationHttpClient = configurationHttpClient;
    }

    public void load(GlobalConfiguration globalConfiguration) {
        final SubscribedConfiguration subscribedConfiguration = createSubscribedConfiguration();
        globalConfiguration.setSubscribedConfiguration(subscribedConfiguration);
        try {
            executeSubscribedFunctions(globalConfiguration);
        } catch (Exception exception) {
            final Logger logger = assets.getLogger(Collections.emptyMap());
            logger.error("Could not schedule function, error: '%s'", exception.getMessage());
        }
    }

    private SubscribedConfiguration createSubscribedConfiguration() {
        final String streamConfigURL = String.format("%s/functional-core/subscribed/config.json", configRepositoryUrl);
        return getSubscribedConfiguration(streamConfigURL);
    }

    private SubscribedConfiguration getSubscribedConfiguration(final String url) {
        return assets.getConfiguration(configurationHttpClient, url, SubscribedConfiguration.class);
    }

    private void executeSubscribedFunctions(final GlobalConfiguration globalConfiguration) {
        // get commands
        final HashCache<String, String> hashCache = globalConfiguration.getCacheConfiguration().getHashCache();
        final CacheStream<String, Map<String, String>> cacheStream = globalConfiguration.getCacheConfiguration().getCacheStream();
        final Subscriber subscriber = globalConfiguration.getCacheConfiguration().getSubscriber();
        final PublishListener publishListener = globalConfiguration.getCacheConfiguration().getPublishListener();
        final MessagePublisher messagePublisher = globalConfiguration.getCacheConfiguration().getMessagePublisher();
        // traverse configurations and create subscriptions
        final GlobalConfiguration.GlobalConfig globalConfig = globalConfiguration.getConfig();
        final String codeRepositoryUrl = globalConfig.getCodeRepositoryUrl();
        final SubscribedConfiguration.Configuration subscribedConfig = globalConfiguration.getSubscribedConfiguration().getConfig();
        final Map<String, SubscribedConfiguration.EnvironmentConfiguration> environments = globalConfiguration.getSubscribedConfiguration().getEnvironments();
        // loop through environments
        for (Map.Entry<String, SubscribedConfiguration.EnvironmentConfiguration> environmentEntry : environments.entrySet()) {
            final String environmentName = environmentEntry.getKey();
            final SubscribedConfiguration.EnvironmentConfiguration environmentConfiguration = environmentEntry.getValue();
            final SubscribedConfiguration.Configuration environmentConfig = environmentConfiguration.getConfig();
            final Map<String, SubscribedConfiguration.TenantConfiguration> tenants = environmentConfiguration.getTenants();
            // loop through tenants
            for (Map.Entry<String, SubscribedConfiguration.TenantConfiguration> tenantEntry : tenants.entrySet()) {
                final String tenantId = tenantEntry.getKey();
                // read subscribed configuration
                final SubscribedConfiguration.TenantConfiguration tenantConfiguration = tenantEntry.getValue();
                final SubscribedConfiguration.Configuration tenantConfig = tenantConfiguration.getConfig();
                final List<SubscribedConfiguration.ConsumerConfiguration> subscribers = tenantConfiguration.getSubscribers();
                for (SubscribedConfiguration.ConsumerConfiguration consumerConfiguration : subscribers) {
                    try {
                        // create context
                        final String logLevel = getSubscribedConsumerLogLevel(globalConfig, subscribedConfig, environmentConfig, tenantConfig, consumerConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        // create consumer function
                        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                        final SubscriptionConsumer subscriptionConsumer = getSubscriptionConsumerFunction(codeRepositoryUrl, consumerConfiguration, query);
                        // subscribe consumer to channel
                        final String channel = consumerConfiguration.getChannel();
                        startSubscriptionConsumerSubscription(environmentName, tenantId, channel, context, subscriptionConsumer, subscriber, publishListener);
                    } catch (Exception e) {
                        // create context
                        final String logLevel = getSubscribedConsumerLogLevel(globalConfig, subscribedConfig, environmentConfig, tenantConfig, consumerConfiguration);
                        final Context context = assets.createContextForEnvironmentAndTenant(environmentName, tenantId, logLevel, hashCache, cacheStream, messagePublisher);
                        context.getLogger().error("Error occurred while creating Subscribed function: %s - %s, error: %s", consumerConfiguration.getName(), consumerConfiguration.getVersion(), e.getMessage());
                    }
                }
            }
        }
    }

    private SubscriptionConsumer getSubscriptionConsumerFunction(
            final String codeServerURL,
            final SubscribedConfiguration.ConsumerConfiguration configuration,
            final String query
    ) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (SubscriptionConsumer.class.isAssignableFrom(object.getClass())) {
                return (SubscriptionConsumer) object;
            } else {
                throw new FunctionIsNotASubscriptionConsumerException(String.format("Resource is not a SubscriptionConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private void startSubscriptionConsumerSubscription(
            final String environment,
            final String tenantId,
            final String channel,
            final Context context,
            final SubscriptionConsumer subscribedConsumer,
            final Subscriber subscriber,
            final PublishListener publishListener) {
        final Logger logger = context.getLogger();
        final String environmentTenantKey = String.format("channel|%s|%s|%s", environment, tenantId, channel);
        try {
            subscriber.subscribe(environmentTenantKey);
            publishListener.register(environmentTenantKey, context, subscribedConsumer);
            logger.debug("Subscription to channel: '%s' created.", environmentTenantKey);
        } catch (RedisException exception) {
            logger.error("Subscription to channel: '%s' could NOT be created.", environmentTenantKey);
        }
    }

    private String getSubscribedConsumerLogLevel(
            final GlobalConfiguration.GlobalConfig globalConfig,
            final SubscribedConfiguration.Configuration streamConfig,
            final SubscribedConfiguration.Configuration environmentConfig,
            final SubscribedConfiguration.Configuration tenantConfig,
            final SubscribedConfiguration.ConsumerConfiguration consumerConfiguration
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
