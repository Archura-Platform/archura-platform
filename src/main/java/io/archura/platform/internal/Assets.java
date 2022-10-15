package io.archura.platform.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.cache.Cache;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.ConfigurationException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.mapper.Mapper;
import io.archura.platform.api.publish.Publisher;
import io.archura.platform.api.stream.LightStream;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.cache.TenantCache;
import io.archura.platform.internal.context.RequestContext;
import io.archura.platform.internal.logging.LoggerFactory;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.publish.TenantPublisher;
import io.archura.platform.internal.stream.CacheStream;
import io.archura.platform.internal.stream.TenantStream;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class Assets {

    private final Map<String, TenantCache> tenantCacheMap = new HashMap<>();
    private final Map<String, TenantStream> tenantStreamMap = new HashMap<>();
    private final Map<String, TenantPublisher> tenantPublisherMap = new HashMap<>();
    private final Map<String, Class<?>> remoteClassMap = new HashMap<>();
    private final Map<String, HttpClient> tenantHttpClientMap = new HashMap<>();
    private final Mapper mapper;
    private final HttpClient defaultHttpClient;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public <T> T getConfiguration(HttpClient configurationHttpClient, String url, Class<T> tClass) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url))
                .build();
        try {
            HttpResponse<InputStream> response = configurationHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() >= 200 && response.statusCode() <= 299) {
                return mapper.readValue(response.body().readAllBytes(), tClass);
            } else {
                final String errorMessage = String.format("Configuration file could not be found, url: %s", url);
                throw new ConfigurationException(errorMessage);
            }
        } catch (IOException | InterruptedException e) {
            String errorMessage = String.format("Error while reading configuration class: '%s' from url: '%s', error: %s", tClass.getSimpleName(), url, e.getMessage());
            throw new ConfigurationException(errorMessage, e);
        }
    }

    public Object createObject(String resourceUrl, String resourceKey, String className, JsonNode jsonNode)
            throws IOException, ReflectiveOperationException {
        if (isNull(remoteClassMap.get(resourceUrl))) {
            final URL url = new URL(resourceKey);
            final URLClassLoader classLoader = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader());
            final Class<?> remoteClass = Class.forName(className, true, classLoader);
            remoteClassMap.put(resourceUrl, remoteClass);
        }
        final Object object = remoteClassMap.get(resourceUrl).getDeclaredConstructor().newInstance();
        configure(jsonNode, object);
        return object;
    }

    public void configure(JsonNode jsonNode, Object object) {
        if (Configurable.class.isAssignableFrom(object.getClass())) {
            final Configurable configurable = (Configurable) object;
            @SuppressWarnings("unchecked") final Map<String, Object> config = mapper.convertValue(jsonNode, Map.class);
            filterFunctionExecutor.execute(configurable, config);
        }
    }

    public void buildContext(
            final Map<String, Object> attributes,
            final HashCache<String, String> hashCache,
            final CacheStream<String, Map<String, String>> cacheStream,
            final MessagePublisher messagePublisher
    ) {
        final RequestContext context = RequestContext.builder()
                .cache(getTenantCache(attributes, hashCache))
                .lightStream(getTenantStream(attributes, cacheStream))
                .publisher(getPublisher(attributes, messagePublisher))
                .logger(getLogger(attributes))
                .httpClient(getHttpClient(attributes))
                .mapper(getMapper(attributes))
                .build();
        attributes.put(Context.class.getSimpleName(), context);
    }

    private Optional<Cache> getTenantCache(final Map<String, Object> attributes, final HashCache<String, String> hashCache) {
        if (attributes.containsKey(GlobalKeys.REQUEST_ENVIRONMENT.getKey())
                && attributes.containsKey(EnvironmentKeys.REQUEST_TENANT_ID.getKey())) {
            final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
            if (isNull(tenantCacheMap.get(environmentTenantIdKey))) {
                final TenantCache tenantCache = new TenantCache(environmentTenantIdKey, hashCache);
                tenantCacheMap.put(environmentTenantIdKey, tenantCache);
            }
            return Optional.of(tenantCacheMap.get(environmentTenantIdKey));
        } else {
            return Optional.empty();
        }
    }

    private Optional<LightStream> getTenantStream(
            final Map<String, Object> attributes,
            final CacheStream<String, Map<String, String>> cacheStream
    ) {
        if (attributes.containsKey(GlobalKeys.REQUEST_ENVIRONMENT.getKey())
                && attributes.containsKey(EnvironmentKeys.REQUEST_TENANT_ID.getKey())) {
            final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
            if (isNull(tenantStreamMap.get(environmentTenantIdKey))) {
                final TenantStream tenantStream = new TenantStream(environmentTenantIdKey, cacheStream);
                tenantStreamMap.put(environmentTenantIdKey, tenantStream);
            }
            return Optional.of(tenantStreamMap.get(environmentTenantIdKey));
        } else {
            return Optional.empty();
        }
    }

    private Optional<Publisher> getPublisher(
            final Map<String, Object> attributes,
            final MessagePublisher messagePublisher
    ) {
        if (attributes.containsKey(GlobalKeys.REQUEST_ENVIRONMENT.getKey())
                && attributes.containsKey(EnvironmentKeys.REQUEST_TENANT_ID.getKey())) {
            final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
            if (isNull(tenantPublisherMap.get(environmentTenantIdKey))) {
                final TenantPublisher tenantPublisher = new TenantPublisher(environmentTenantIdKey, messagePublisher);
                tenantPublisherMap.put(environmentTenantIdKey, tenantPublisher);
            }
            return Optional.of(tenantPublisherMap.get(environmentTenantIdKey));
        } else {
            return Optional.empty();
        }
    }

    public Logger getLogger(final Map<String, Object> attributes) {
        return LoggerFactory.create(attributes);
    }

    private HttpClient getHttpClient(final Map<String, Object> attributes) {
        final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
        if (isNull(tenantHttpClientMap.get(environmentTenantIdKey))) {
            tenantHttpClientMap.put(environmentTenantIdKey, defaultHttpClient);
        }
        return tenantHttpClientMap.get(environmentTenantIdKey);
    }

    private Mapper getMapper(Map<String, Object> attributes) {
        return this.mapper;
    }

    private String getEnvironmentTenantKey(Map<String, Object> attributes) {
        final String environmentName = String.valueOf(attributes.getOrDefault(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), GlobalKeys.ENVIRONMENT_NOT_SET.getKey()));
        final String tenantId = String.valueOf(attributes.getOrDefault(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.TENANT_NOT_SET.getKey()));
        return String.format("%s|%s", environmentName, tenantId);
    }

    public Context createContextForEnvironmentAndTenant(
            final String environmentName,
            final String tenantId,
            final String logLevel,
            final HashCache<String, String> hashCache,
            final CacheStream<String, Map<String, String>> cacheStream,
            final MessagePublisher messagePublisher
    ) {
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), environmentName);
        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), tenantId);
        if (nonNull(logLevel)) {
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
        }
        buildContext(attributes, hashCache, cacheStream, messagePublisher);
        return (Context) attributes.get(Context.class.getSimpleName());
    }

}
