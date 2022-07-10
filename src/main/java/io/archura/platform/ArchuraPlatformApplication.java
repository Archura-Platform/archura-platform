package io.archura.platform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.configuration.EnvironmentConfiguration;
import io.archura.platform.configuration.FunctionConfiguration;
import io.archura.platform.configuration.GlobalConfiguration;
import io.archura.platform.configuration.PostFilterConfiguration;
import io.archura.platform.configuration.PreFilterConfiguration;
import io.archura.platform.configuration.TenantConfiguration;
import io.archura.platform.exception.ConfigurationException;
import io.archura.platform.exception.ResourceLoadException;
import io.archura.platform.function.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.isNull;

@SpringBootApplication
public class ArchuraPlatformApplication {

    private static final String DEFAULT_ENVIRONMENT = "default";
    private static final String DEFAULT_TENANT_ID = "default";
    private static final String CATCH_ALL_KEY = "*";

    private static final ExecutorService HTTP_CLIENT_EXECUTOR = Executors.newCachedThreadPool();

    @Value("${config.repository.url:http://config-service/}")
    private String configRepositoryUrl;

    @Value("${code.repository.url:http://code-service/}")
    private String codeRepositoryUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final GlobalConfiguration globalConfiguration = new GlobalConfiguration();

    private final Map<String, HandlerFunction<ServerResponse>> functionMap = new ConcurrentHashMap<>();

    private final Map<String, Consumer<ServerRequest>> preFilterMap = new ConcurrentHashMap<>();

    private final Map<String, BiConsumer<ServerRequest, ServerResponse>> postFilterMap = new ConcurrentHashMap<>();
    private final Map<String, Class<?>> remoteClassMap = new HashMap<>();
    private Map<String, TenantCache> tenantCacheMap = new HashMap<>();


    public static void main(String[] args) {
        SpringApplication.run(ArchuraPlatformApplication.class, args);
    }

    @Bean
    public RouterFunction<ServerResponse> routes(HashOperations<String, String, Map<String, Object>> hashOperations) {
        return RouterFunctions.route()
                .route(RequestPredicates.all(), request -> {
                    try {
                        getGlobalPreFilters().forEach(c -> c.accept(request));

                        String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                        getEnvironmentPreFilters(environmentName).forEach(c -> c.accept(request));

                        String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
                        getTenantPreFilters(environmentName, tenantId).forEach(c -> c.accept(request));

                        final String routeId = request.attribute("ROUTE_ID").map(String::valueOf).orElse(CATCH_ALL_KEY);
                        final String tenantCacheKey = String.format("%s|%s", environmentName, tenantId);
                        final TenantCache tenantCache = tenantCacheMap.getOrDefault(tenantCacheKey, new TenantCache(tenantCacheKey, hashOperations));
                        request.attributes().put(Cache.class.getSimpleName(), tenantCache);
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
        if (!preFilterMap.containsKey(resourceKey)) {
            try {
                final Object object = createObject(resourceUrl, configuration.getName(), configuration.getConfig());
                @SuppressWarnings("unchecked") final Consumer<ServerRequest> filter = (Consumer<ServerRequest>) object;
                preFilterMap.put(resourceKey, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return preFilterMap.get(resourceKey);
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
        final Map<String, FunctionConfiguration> routeFunctions = tenantConfiguration.getRouteFunctions();
        return Optional.ofNullable(routeFunctions.get(routeId))
                .map(config -> getFunction(codeRepositoryUrl, config, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)));
    }

    private HandlerFunction<ServerResponse> getFunction(String codeServerURL, FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        if (!functionMap.containsKey(resourceKey)) {
            try {
                final Object object = createObject(resourceUrl, configuration.getName(), configuration.getConfig());
                @SuppressWarnings("unchecked") final HandlerFunction<ServerResponse> handlerFunction = (HandlerFunction<ServerResponse>) object;
                functionMap.put(resourceKey, handlerFunction);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return functionMap.get(resourceKey);
    }

    private Object createObject(String resourceUrl, String className, JsonNode jsonNode)
            throws IOException, ReflectiveOperationException {
        if (isNull(remoteClassMap.get(resourceUrl))) {
            final URL url = new URL(resourceUrl);
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
        if (!postFilterMap.containsKey(resourceKey)) {
            try {
                final Object object = createObject(resourceUrl, configuration.getName(), configuration.getConfig());
                @SuppressWarnings("unchecked") final BiConsumer<ServerRequest, ServerResponse> filter = (BiConsumer<ServerRequest, ServerResponse>) object;
                postFilterMap.put(resourceKey, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return postFilterMap.get(resourceKey);
    }

    private <T> T getConfiguration(String url, Class<T> tClass) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .executor(HTTP_CLIENT_EXECUTOR)
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url))
                .build();
        try {
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
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
        addErrorHeaders(t, bodyBuilder);
        return request.attribute("RESPONSE_MESSAGE")
                .map(bodyBuilder::body)
                .orElse(bodyBuilder.build());
    }

    private void addErrorHeaders(Throwable t, ServerResponse.BodyBuilder bodyBuilder) {
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
        bodyBuilder.header("X-A-Error-Type", String.join(", ", String.join(",", errorTypes)));
        bodyBuilder.header("X-A-Error-Message", String.join(", ", String.join(",", errorMessages)));
    }

}
