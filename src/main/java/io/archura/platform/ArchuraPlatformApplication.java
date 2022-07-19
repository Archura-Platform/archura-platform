package io.archura.platform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.attribute.EnvironmentKeys;
import io.archura.platform.attribute.GlobalKeys;
import io.archura.platform.attribute.TenantKeys;
import io.archura.platform.cache.Cache;
import io.archura.platform.cache.TenantCache;
import io.archura.platform.configuration.EnvironmentConfiguration;
import io.archura.platform.configuration.FunctionConfiguration;
import io.archura.platform.configuration.GlobalConfiguration;
import io.archura.platform.configuration.PostFilterConfiguration;
import io.archura.platform.configuration.PreFilterConfiguration;
import io.archura.platform.configuration.TenantConfiguration;
import io.archura.platform.context.Context;
import io.archura.platform.context.RequestContext;
import io.archura.platform.exception.ConfigurationException;
import io.archura.platform.exception.ErrorDetail;
import io.archura.platform.exception.FunctionIsNotAHandlerFunctionException;
import io.archura.platform.exception.PostFilterIsNotABiConsumerException;
import io.archura.platform.exception.PreFilterIsNotAConsumerException;
import io.archura.platform.exception.ResourceLoadException;
import io.archura.platform.function.Configurable;
import io.archura.platform.logging.Logger;
import io.archura.platform.logging.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.RequestPredicates;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@SpringBootApplication
public class ArchuraPlatformApplication {

    @Value("${config.repository.url:http://config-service/}")
    private String configRepositoryUrl;

    @Value("${code.repository.url:http://code-service/}")
    private String codeRepositoryUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final GlobalConfiguration globalConfiguration = new GlobalConfiguration();
    private final Map<String, TenantCache> tenantCacheMap = new HashMap<>();
    private final Map<String, Logger> tenantLoggerMap = new HashMap<>();
    private final Map<String, Class<?>> remoteClassMap = new HashMap<>();
    private final Map<String, HttpClient> tenantHttpClientMap = new HashMap<>();

    private final HttpClient configurationHttpClient = buildDefaultHttpClient();

    public static void main(String[] args) {
        SpringApplication.run(ArchuraPlatformApplication.class, args);
    }

    @Bean
    public TomcatServletWebServerFactory tomcatContainerFactory() {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.setTomcatProtocolHandlerCustomizers(
                Collections.singletonList(tomcatProtocolHandlerCustomizer())
        );
        return factory;
    }

    @Bean
    public TomcatProtocolHandlerCustomizer<?> tomcatProtocolHandlerCustomizer() {
        final ThreadFactory factory = Thread.ofVirtual().name("VIRTUAL-THREAD").factory();
        final ExecutorService executorService = Executors.newCachedThreadPool(factory);
        return protocolHandler -> protocolHandler.setExecutor(executorService);
    }

    @Bean
    public RouterFunction<ServerResponse> routes(HashOperations<String, String, Map<String, Object>> hashOperations) {
        return RouterFunctions.route()
                .route(RequestPredicates.all(), request -> {
                    try {
                        final Map<String, Object> attributes = request.attributes();
                        rebuildContext(attributes, hashOperations);

                        getGlobalPreFilters().forEach(c -> c.accept(request));
                        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), GlobalKeys.DEFAULT_ENVIRONMENT.getKey());
                        rebuildContext(attributes, hashOperations);

                        String environmentName = String.valueOf(attributes.get(GlobalKeys.REQUEST_ENVIRONMENT.getKey()));
                        getEnvironmentPreFilters(environmentName).forEach(c -> c.accept(request));
                        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.DEFAULT_TENANT_ID.getKey());
                        rebuildContext(attributes, hashOperations);

                        String tenantId = String.valueOf(attributes.get(EnvironmentKeys.REQUEST_TENANT_ID.getKey()));
                        getTenantPreFilters(environmentName, tenantId).forEach(c -> c.accept(request));

                        final String routeId = request.attribute(TenantKeys.ROUTE_ID.getKey()).map(String::valueOf).orElse(TenantKeys.CATCH_ALL_ROUTE_KEY.getKey());

                        final ServerResponse response = getTenantFunctions(environmentName, tenantId, routeId)
                                .orElse(r -> ServerResponse
                                        .notFound()
                                        .header(String.format("X-A-NotFound-%s-%s-%s", environmentName, tenantId, routeId))
                                        .build()
                                )
                                .handle(request);

                        getTenantPostFilters(environmentName, tenantId).forEach(bc -> bc.accept(request, response));
                        getEnvironmentPostFilters(environmentName).forEach(bc -> bc.accept(request, response));
                        getGlobalPostFilters().forEach(bc -> bc.accept(request, response));
                        return response;
                    } catch (Exception e) {
                        return this.getErrorResponse(e, request);
                    }
                })
                .build();
    }

    private void rebuildContext(Map<String, Object> attributes, HashOperations<String, String, Map<String, Object>> hashOperations) {
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

    private Logger getLogger(final Map<String, Object> attributes) {
        final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
        if (isNull(tenantLoggerMap.get(environmentTenantIdKey))) {
            tenantLoggerMap.put(environmentTenantIdKey, LoggerFactory.create(attributes));
        }
        return tenantLoggerMap.get(environmentTenantIdKey);
    }

    private HttpClient getHttpClient(final Map<String, Object> attributes) {
        final String environmentTenantIdKey = getEnvironmentTenantKey(attributes);
        if (isNull(tenantHttpClientMap.get(environmentTenantIdKey))) {
            tenantHttpClientMap.put(environmentTenantIdKey, buildDefaultHttpClient());
        }
        return tenantHttpClientMap.get(environmentTenantIdKey);
    }

    private HttpClient buildDefaultHttpClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    }

    private String getEnvironmentTenantKey(Map<String, Object> attributes) {
        final String environmentName = String.valueOf(attributes.getOrDefault(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), GlobalKeys.ENVIRONMENT_NOT_SET.getKey()));
        final String tenantId = String.valueOf(attributes.getOrDefault(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.TENANT_NOT_SET.getKey()));
        return String.format("%s|%s", environmentName, tenantId);
    }

    private List<Consumer<ServerRequest>> getGlobalPreFilters() {
        if (globalConfiguration.getPre().isEmpty()) {
            String globalConfigURL = String.format("%s/global/config.json", configRepositoryUrl);
            GlobalConfiguration globalConfig = getGlobalConfiguration(globalConfigURL);
            this.globalConfiguration.setPre(globalConfig.getPre());
            this.globalConfiguration.setPost(globalConfig.getPost());
        }
        return globalConfiguration
                .getPre()
                .stream()
                .map(preFilterConfig -> getPreFilter(codeRepositoryUrl, preFilterConfig, "global"))
                .toList();
    }

    private GlobalConfiguration getGlobalConfiguration(String url) {
        return getConfiguration(url, GlobalConfiguration.class);
    }

    private List<Consumer<ServerRequest>> getEnvironmentPreFilters(String environmentName) {
        final EnvironmentConfiguration environmentConfiguration = globalConfiguration.getEnvironments().get(environmentName);
        if (isNull(environmentConfiguration)) {
            String environmentConfigURL = String.format("%s/environments/%s/config.json", configRepositoryUrl, environmentName);
            EnvironmentConfiguration environmentConfig = getEnvironmentConfiguration(environmentConfigURL);
            globalConfiguration.getEnvironments().put(environmentName, environmentConfig);
        }
        return globalConfiguration.getEnvironments().get(environmentName)
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s", environmentName)))
                .toList();
    }

    private EnvironmentConfiguration getEnvironmentConfiguration(String url) {
        return getConfiguration(url, EnvironmentConfiguration.class);
    }

    private List<Consumer<ServerRequest>> getTenantPreFilters(String environmentName, String tenantId) {
        final EnvironmentConfiguration environmentConfiguration = globalConfiguration.getEnvironments().get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        final TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            String tenantConfigURL = String.format("%s/environments/%s/tenants/%s/config.json", configRepositoryUrl, environmentName, tenantId);
            TenantConfiguration tenantConfig = getTenantConfiguration(tenantConfigURL);
            environmentConfiguration.getTenants().put(tenantId, tenantConfig);
        }
        return environmentConfiguration.getTenants().get(tenantId)
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)))
                .toList();
    }

    private TenantConfiguration getTenantConfiguration(String url) {
        return getConfiguration(url, TenantConfiguration.class);
    }

    private Consumer<ServerRequest> getPreFilter(String codeServerURL, PreFilterConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (Consumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final Consumer<ServerRequest> consumer = (Consumer<ServerRequest>) object;
                return consumer;
            } else {
                throw new PreFilterIsNotAConsumerException(String.format("Resource is not a Consumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private Optional<HandlerFunction<ServerResponse>> getTenantFunctions(String environmentName, String tenantId, String routeId) {
        final EnvironmentConfiguration environmentConfiguration = globalConfiguration.getEnvironments().get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Optional.empty();
        }
        final TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            return Optional.empty();
        }
        final String query = String.format("environmentName=%s&tenantId=%s", environmentName, tenantId);
        final Map<String, FunctionConfiguration> routeFunctions = tenantConfiguration.getRouteFunctions();
        final FunctionConfiguration routeFunctionConfig = routeFunctions.get(routeId);
        if (nonNull(routeFunctionConfig)) {
            return Optional.of(getFunction(codeRepositoryUrl, routeFunctionConfig, query));
        }
        final FunctionConfiguration catchAllRouteFunctionConfig = routeFunctions.get(TenantKeys.CATCH_ALL_ROUTE_KEY.getKey());
        if (nonNull(catchAllRouteFunctionConfig)) {
            return Optional.of(getFunction(codeRepositoryUrl, catchAllRouteFunctionConfig, query));
        }
        return Optional.empty();
    }

    private HandlerFunction<ServerResponse> getFunction(String codeServerURL, FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (HandlerFunction.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final HandlerFunction<ServerResponse> handlerFunction = (HandlerFunction<ServerResponse>) object;
                return handlerFunction;
            } else {
                throw new FunctionIsNotAHandlerFunctionException(String.format("Resource is not a HandlerFunction, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private Object createObject(String resourceUrl, String resourceKey, String className, JsonNode jsonNode)
            throws IOException, ReflectiveOperationException {
        if (isNull(remoteClassMap.get(resourceUrl))) {
            final URL url = new URL(resourceKey);
            final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
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

    private List<BiConsumer<ServerRequest, ServerResponse>> getTenantPostFilters(String environmentName, String tenantId) {
        final EnvironmentConfiguration environmentConfiguration = globalConfiguration.getEnvironments().get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        final TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            return Collections.emptyList();
        }
        return tenantConfiguration
                .getPost()
                .stream()
                .map(preFilter -> getPostFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)))
                .toList();
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getEnvironmentPostFilters(String environmentName) {
        final EnvironmentConfiguration environmentConfiguration = globalConfiguration.getEnvironments().get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        return environmentConfiguration
                .getPost()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter, String.format("environmentName=%s", environmentName)))
                .toList();
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getGlobalPostFilters() {
        return globalConfiguration
                .getPost()
                .stream()
                .map(postFilterConfig -> getPostFilter(codeRepositoryUrl, postFilterConfig, "global"))
                .toList();
    }

    private BiConsumer<ServerRequest, ServerResponse> getPostFilter(String codeServerURL, PostFilterConfiguration configuration, String query) {
        String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (BiConsumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final BiConsumer<ServerRequest, ServerResponse> filter = (BiConsumer<ServerRequest, ServerResponse>) object;
                return filter;
            } else {
                throw new PostFilterIsNotABiConsumerException(String.format("Resource is not a BiConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private <T> T getConfiguration(String url, Class<T> tClass) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url))
                .build();
        try {
            HttpResponse<InputStream> response = configurationHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            return objectMapper.readValue(response.body(), tClass);
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    private ServerResponse getErrorResponse(Throwable t, ServerRequest request) {
        final HttpStatus httpStatus = request.attribute("RESPONSE_HTTP_STATUS")
                .map(code -> HttpStatus.valueOf(String.valueOf(code)))
                .orElse(HttpStatus.INTERNAL_SERVER_ERROR);
        final ServerResponse.BodyBuilder bodyBuilder = ServerResponse.status(httpStatus);
        ErrorDetail errorDetail = getErrorDetails(t);
        addErrorHeaders(errorDetail, bodyBuilder);
        logErrorDetails(errorDetail, request.attributes());
        return request.attribute("RESPONSE_MESSAGE")
                .map(bodyBuilder::body)
                .orElse(bodyBuilder.build());
    }

    private void logErrorDetails(final ErrorDetail errorDetail, final Map<String, Object> attributes) {
        final String errorMessage = String.format("%s %s", errorDetail.getErrorTypes(), errorDetail.getErrorMessages());
        getLogger(attributes).error(errorMessage);
    }

    private ErrorDetail getErrorDetails(Throwable t) {
        final List<String> errorTypes = new ArrayList<>();
        final List<String> errorMessages = new ArrayList<>();
        errorTypes.add(t.getClass().getSimpleName());
        errorMessages.add(t.getMessage());
        Throwable cause = t.getCause();
        while (Objects.nonNull(cause)) {
            errorMessages.add(cause.getMessage());
            errorTypes.add(cause.getClass().getSimpleName());
            cause = cause.getCause();
        }
        return new ErrorDetail(errorTypes, errorMessages);
    }

    private void addErrorHeaders(ErrorDetail errorDetail, ServerResponse.BodyBuilder bodyBuilder) {
        bodyBuilder.header("X-A-Error-Type", String.join(", ", String.join(",", errorDetail.getErrorTypes())));
        bodyBuilder.header("X-A-Error-Message", String.join(", ", String.join(",", errorDetail.getErrorMessages())));
    }

}
