package io.archura.platform.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.cache.Cache;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.ConfigurationException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.internal.cache.TenantCache;
import io.archura.platform.internal.context.RequestContext;
import io.archura.platform.internal.logging.LoggerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.HashOperations;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.isNull;

@RequiredArgsConstructor
public class Assets {

    private final Map<String, TenantCache> tenantCacheMap = new HashMap<>();
    private final Map<String, Logger> tenantLoggerMap = new HashMap<>();
    private final Map<String, Class<?>> remoteClassMap = new HashMap<>();
    private final Map<String, HttpClient> tenantHttpClientMap = new HashMap<>();
    private final ObjectMapper objectMapper;
    private final HttpClient defaultHttpClient;

    public <T> T getConfiguration(HttpClient configurationHttpClient, String url, Class<T> tClass) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url))
                .build();
        try {
            HttpResponse<InputStream> response = configurationHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() >= 200 && response.statusCode() <= 299) {
                return objectMapper.readValue(response.body(), tClass);
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
        if (Configurable.class.isAssignableFrom(object.getClass())) {
            final Configurable configurable = (Configurable) object;
            final Map<String, Object> config = objectMapper.convertValue(jsonNode, new TypeReference<>() {
            });
            configurable.setConfiguration(Collections.unmodifiableMap(config));
        }
        return object;
    }

    public void rebuildContext(Map<String, Object> attributes, HashOperations<String, String, Map<String, Object>> hashOperations) {
        final Object contextObject = attributes.get(Context.class.getSimpleName());
        if (isNull(contextObject)) {
            attributes.put(Context.class.getSimpleName(), RequestContext.builder().build());
        }
        final RequestContext currentContext = (RequestContext) attributes.get(Context.class.getSimpleName());
        final RequestContext initialContext = currentContext
                .toBuilder()
                .cache(getTenantCache(attributes, hashOperations))
                .logger(getLogger(attributes))
                .httpClient(getHttpClient(attributes))
                .objectMapper(getObjectMapper(attributes))
                .build();
        attributes.put(Context.class.getSimpleName(), initialContext);
    }

    private Optional<Cache> getTenantCache(final Map<String, Object> attributes, final HashOperations<String, String, Map<String, Object>> hashOperations) {
        if (attributes.containsKey(GlobalKeys.REQUEST_ENVIRONMENT.getKey())
                && attributes.containsKey(EnvironmentKeys.REQUEST_TENANT_ID.getKey())) {
            final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
            if (isNull(tenantCacheMap.get(environmentTenantIdKey))) {
                final TenantCache tenantCache = new TenantCache(environmentTenantIdKey, hashOperations);
                tenantCacheMap.put(environmentTenantIdKey, tenantCache);
            }
            return Optional.of(tenantCacheMap.get(environmentTenantIdKey));
        } else {
            return Optional.empty();
        }
    }

    public Logger getLogger(final Map<String, Object> attributes) {
        final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
        if (isNull(tenantLoggerMap.get(environmentTenantIdKey))) {
            tenantLoggerMap.put(environmentTenantIdKey, LoggerFactory.create(attributes));
        }
        return tenantLoggerMap.get(environmentTenantIdKey);
    }

    private HttpClient getHttpClient(final Map<String, Object> attributes) {
        final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
        if (isNull(tenantHttpClientMap.get(environmentTenantIdKey))) {
            tenantHttpClientMap.put(environmentTenantIdKey, defaultHttpClient);
        }
        return tenantHttpClientMap.get(environmentTenantIdKey);
    }

    private ObjectMapper getObjectMapper(Map<String, Object> attributes) {
        return objectMapper;
    }

    private String getEnvironmentTenantKey(Map<String, Object> attributes) {
        final String environmentName = String.valueOf(attributes.getOrDefault(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), GlobalKeys.ENVIRONMENT_NOT_SET.getKey()));
        final String tenantId = String.valueOf(attributes.getOrDefault(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.TENANT_NOT_SET.getKey()));
        return String.format("%s|%s", environmentName, tenantId);
    }

}
