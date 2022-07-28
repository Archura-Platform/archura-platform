package io.archura.platform.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.FunctionIsNotAContextConsumerException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.internal.configuration.CacheConfiguration;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.configuration.IIFEConfiguration;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.data.redis.core.HashOperations;

import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
public class Initializer {

    private final String configRepositoryUrl;
    private final HttpClient configurationHttpClient;
    private final ConfigurableBeanFactory beanFactory;
    private final ExecutorService executorService;
    private final Assets assets;

    public void initialize() {
        final GlobalConfiguration globalConfiguration = loadGlobalConfiguration(beanFactory);
        handleIFFEFunctions(globalConfiguration);
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

    private void handleIFFEFunctions(final GlobalConfiguration globalConfiguration) {
        final IIFEConfiguration iifeConfiguration = createIIFEConfiguration();
        globalConfiguration.setIifeConfiguration(iifeConfiguration);
        executeIIFEFunctions(globalConfiguration);
    }

    private IIFEConfiguration createIIFEConfiguration() {
        final String iffeConfigURL = String.format("%s/functional-core/iife/config.json", configRepositoryUrl);
        return getIFFEConfiguration(iffeConfigURL);
    }

    private IIFEConfiguration getIFFEConfiguration(String url) {
        return assets.getConfiguration(configurationHttpClient, url, IIFEConfiguration.class);
    }

    private void executeIIFEFunctions(GlobalConfiguration globalConfiguration) {
        final IIFEConfiguration.Configuration config = globalConfiguration.getIifeConfiguration().getConfig();
        final Map<String, IIFEConfiguration.EnvironmentConfiguration> environments = globalConfiguration.getIifeConfiguration().getEnvironments();
        for (Map.Entry<String, IIFEConfiguration.EnvironmentConfiguration> environmentEntry : environments.entrySet()) {
            final String environmentName = environmentEntry.getKey();
            final IIFEConfiguration.EnvironmentConfiguration environmentConfiguration = environmentEntry.getValue();
            final IIFEConfiguration.Configuration environmentConfig = environmentConfiguration.getConfig(); // set log level
            final Map<String, IIFEConfiguration.TenantConfiguration> tenants = environmentConfiguration.getTenants();
            for (Map.Entry<String, IIFEConfiguration.TenantConfiguration> tenantEntry : tenants.entrySet()) {
                final String tenantId = tenantEntry.getKey();
                // create context
                final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
                final Context context = createContextForEnvironmentAndTenant(hashOperations, environmentName, tenantId);
                // read IFFE configuration
                final IIFEConfiguration.TenantConfiguration tenantConfiguration = tenantEntry.getValue();
                final IIFEConfiguration.Configuration tenantConfig = tenantConfiguration.getConfig(); // set log level
                final List<IIFEConfiguration.FunctionConfiguration> functions = tenantConfiguration.getFunctions();
                for (IIFEConfiguration.FunctionConfiguration functionConfiguration : functions) {
                    // create function
                    final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
                    final String codeRepositoryUrl = globalConfiguration.getConfig().getCodeRepositoryUrl();
                    final ContextConsumer iifeFunction = getIIFEFunction(codeRepositoryUrl, functionConfiguration, query);
                    // invoke function
                    executorService.submit(() -> iifeFunction.accept(context));
                }
            }
        }
    }

    private ContextConsumer getIIFEFunction(String codeServerURL, IIFEConfiguration.FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (ContextConsumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final ContextConsumer handlerFunction = (ContextConsumer) object;
                return handlerFunction;
            } else {
                throw new FunctionIsNotAContextConsumerException(String.format("Resource is not a ContextConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private Context createContextForEnvironmentAndTenant(HashOperations<String, String, Map<String, Object>> hashOperations, String environmentName, String tenantId) {
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), environmentName);
        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), tenantId);
        assets.rebuildContext(attributes, hashOperations);
        return (Context) attributes.get(Context.class.getSimpleName());
    }

    private void handleStreamFunctions(GlobalConfiguration globalConfiguration) {
        // create stream configuration
        // set stream configuration to global config
        // start/register stream functions
    }

    private void handleScheduledFunctions(GlobalConfiguration globalConfiguration) {
        // create scheduled configuration
        // set scheduled configuration to global config
        // schedule functions
    }

}
