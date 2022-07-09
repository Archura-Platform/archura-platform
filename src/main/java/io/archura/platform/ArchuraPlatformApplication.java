package io.archura.platform;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.configuration.Configuration;
import io.archura.platform.configuration.EnvironmentConfiguration;
import io.archura.platform.configuration.FilterConfiguration;
import io.archura.platform.configuration.FunctionConfiguration;
import io.archura.platform.configuration.FunctionsConfiguration;
import io.archura.platform.configuration.GlobalConfiguration;
import io.archura.platform.configuration.PostFilter;
import io.archura.platform.configuration.PreFilter;
import io.archura.platform.configuration.TenantConfiguration;
import io.archura.platform.exception.JsonReadException;
import io.archura.platform.exception.ResourceLoadException;
import org.reactivestreams.Subscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.HandlerFunction;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.isNull;

@SpringBootApplication
public class ArchuraPlatformApplication {

    public static final String MAIN_CLASS = "Main-Class";
    public static final String DEFAULT_ENVIRONMENT = "default";
    public static final String DEFAULT_TENANT_ID = "default";
    public static final String CATCH_ALL_KEY = "*";
    public static final String X_A_HEADER_KEY_PLATFORM_TOKEN = "X-A-PlatformToken";
    @Value("${config.repository.url:http://config-service/}")
    private String configRepositoryUrl;

    @Value("${archura.platform.token}")
    private String platformToken;

    @Value("${code.repository.url:http://code-service/}")
    private String codeRepositoryUrl;

    private Configuration configuration = new Configuration();

    public static final String MANIFEST_MF = "META-INF/MANIFEST.MF";
    private final Map<String, HandlerFunction<ServerResponse>> functionMap = new ConcurrentHashMap<>();

    private final Map<String, Consumer<ServerRequest>> preFilterMap = new ConcurrentHashMap<>();
    private final Map<String, BiConsumer<ServerRequest, ServerResponse>> postFilterMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        SpringApplication.run(ArchuraPlatformApplication.class, args);
    }

    /**
     * Entry point for all requests.
     *
     * @return Router function that handles all the requests.
     */
    @Bean
    public RouterFunction<ServerResponse> routes(
            final ReactiveHashOperations<String, String, Map<String, Object>> hashOperations
    ) {
        return RouterFunctions.route()
                .before(request -> {
                    getGlobalPreFilters()
                            .doOnNext(serverRequestConsumer -> serverRequestConsumer.accept(request))
                            .subscribe();

                    String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                    getEnvironmentPreFilters(environmentName)
                            .doOnNext(serverRequestConsumer -> serverRequestConsumer.accept(request))
                            .subscribe();
//
                    String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
                    getTenantPreFilters(environmentName, tenantId)
                            .doOnNext(serverRequestConsumer -> serverRequestConsumer.accept(request))
                            .subscribe();

                    return request;
                })
                .route(RequestPredicates.all(), request -> {
                    final String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                    final String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
                    final String routeId = request.attribute("ROUTE_ID").map(String::valueOf).orElse(CATCH_ALL_KEY);

                    return getTenantFunctions(environmentName, tenantId, routeId)
                            .flatMap(function -> function.handle(request));
                })
                .after((request, response) -> {
                    String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                    String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);

                    getTenantPostFilters(environmentName, tenantId)
                            .doOnNext(bc -> bc.accept(request, response))
                            .subscribe();

                    getEnvironmentPostFilters(environmentName)
                            .doOnNext(bc -> bc.accept(request, response))
                            .subscribe();

                    getGlobalPostFilters()
                            .doOnNext(bc -> bc.accept(request, response))
                            .subscribe();
                    return response;
                })
                .onError(Throwable.class, this::getErrorResponse)
                .build();
    }

    private Flux<Consumer<ServerRequest>> getGlobalPreFilters() {
        if (isNull(configuration.getGlobalConfiguration())) {
            final String globalFiltersUrl = String.format("%s/global/filters.json", configRepositoryUrl);
            return Flux.from(subscriber ->
                    fetchConfiguration(globalFiltersUrl, GlobalConfiguration.class)
                            .subscribe(filterConfig -> {
                                configuration.setGlobalConfiguration(filterConfig);
                                subscribePreFilter(filterConfig, subscriber);
                            }));
        } else {
            return Flux.from(subscriber -> subscribePreFilter(configuration.getGlobalConfiguration(), subscriber));
        }
    }

    private Flux<Consumer<ServerRequest>> getEnvironmentPreFilters(String environmentName) {
        if (isNull(configuration.getEnvironmentConfigurations().get(environmentName))) {
            final String environmentFiltersURL = String.format("%s/environments/%s/filters.json", configRepositoryUrl, environmentName);
            return Flux.from(subscriber ->
                    fetchConfiguration(environmentFiltersURL, EnvironmentConfiguration.class)
                            .subscribe(filterConfig -> {
                                configuration.getEnvironmentConfigurations().put(environmentName, filterConfig);
                                subscribePreFilter(filterConfig, subscriber);
                            }));
        } else {
            return Flux.from(subscriber -> subscribePreFilter(configuration.getEnvironmentConfigurations().get(environmentName), subscriber));
        }
    }

    private Flux<Consumer<ServerRequest>> getTenantPreFilters(String environmentName, String tenantId) {
        if (!configuration.getEnvironmentConfigurations().containsKey(environmentName)) {
            return Flux.empty();
        }
        final EnvironmentConfiguration environmentConfiguration = configuration.getEnvironmentConfigurations().get(environmentName);
        final Map<String, TenantConfiguration> tenantConfigurations = environmentConfiguration.getTenantConfigurations();
        final TenantConfiguration tenantConfiguration = tenantConfigurations.get(tenantId);
        if (isNull(tenantConfiguration)) {
            String tenantFiltersURL = String.format("%s/environments/%s/tenants/%s/filters.json", configRepositoryUrl, environmentName, tenantId);
            return Flux.from(subscriber ->
                    fetchConfiguration(tenantFiltersURL, TenantConfiguration.class)
                            .subscribe(filterConfig -> {
                                tenantConfigurations.put(tenantId, filterConfig);
                                subscribePreFilter(filterConfig, subscriber);
                            }));
        } else {
            return Flux.from(subscriber -> subscribePreFilter(tenantConfiguration, subscriber));
        }
    }

    private <T> Mono<T> fetchConfiguration(String url, Class<T> valueType) {
        System.out.println("url = " + url);
        return WebClient.create()
                .get()
                .uri(URI.create(url))
                .header(X_A_HEADER_KEY_PLATFORM_TOKEN, platformToken)
                .exchangeToMono(clientResponse -> clientResponse.bodyToMono(String.class))
                .doOnError(Throwable.class, throwable -> System.out.println("throwable = " + throwable))
                .map(response -> readFilterConfiguration(valueType, response));
    }

    private <T> T readFilterConfiguration(Class<T> valueType, String response) {
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(response, valueType);
        } catch (JacksonException e) {
            throw new JsonReadException(e);
        }
    }

    private void subscribePreFilter(FilterConfiguration filterConfiguration, Subscriber<? super Consumer<ServerRequest>> subscriber) {
        filterConfiguration.getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                .forEach(subscriber::onNext);
    }

    private Consumer<ServerRequest> getPreFilter(String codeServerURL, PreFilter preFilter) {
        String filterURL = String.format("%s/%s-%s.jar", codeServerURL, preFilter.getName(), preFilter.getVersion());
        if (!preFilterMap.containsKey(filterURL)) {
            System.out.println("getPreFilter filterURL = " + filterURL);
            try {
                final URL url = URI.create(filterURL).toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final Class<Consumer<ServerRequest>> classToLoad = (Class<Consumer<ServerRequest>>) Class.forName(preFilter.getName(), true, classLoader);
                final Consumer<ServerRequest> filter = classToLoad.getDeclaredConstructor().newInstance();
                preFilterMap.put(filterURL, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return preFilterMap.get(filterURL);
    }

    private Mono<HandlerFunction<ServerResponse>> getTenantFunctions(final String environmentName, final String tenantId, final String routeId) {
        if (isNull(configuration.getFunctionsConfiguration().getRouteFunctions().get(routeId))) {
            String tenantFunctionsURL = String.format("%s/environments/%s/tenants/%s/functions.json", configRepositoryUrl, environmentName, tenantId);
            System.out.println("tenantFunctionsURL = " + tenantFunctionsURL);
            return fetchConfiguration(tenantFunctionsURL, FunctionsConfiguration.class)
                    .doOnNext(functionsConfiguration -> configuration.setFunctionsConfiguration(functionsConfiguration))
                    .filter(functionsConfiguration -> functionsConfiguration.getRouteFunctions().containsKey(routeId))
                    .map(functionsConfiguration -> functionsConfiguration.getRouteFunctions().get(routeId))
                    .map(functionConfiguration -> getFunction(codeRepositoryUrl, functionConfiguration))
                    .switchIfEmpty(Mono.just(request -> ServerResponse.notFound().build()));
        } else {
            if (configuration.getFunctionsConfiguration().getRouteFunctions().containsKey(routeId)) {
                final FunctionConfiguration functionConfiguration = configuration.getFunctionsConfiguration().getRouteFunctions().get(routeId);
                return Mono.just(getFunction(codeRepositoryUrl, functionConfiguration));
            } else {
                return Mono.just(request -> ServerResponse.notFound().build());
            }
        }
    }

    private HandlerFunction<ServerResponse> getFunction(String codeServerURL, FunctionConfiguration functionConfiguration) {
        String functionURL = String.format("%s/%s-%s.jar", codeServerURL, functionConfiguration.getName(), functionConfiguration.getVersion());
        if (!functionMap.containsKey(functionURL)) {
            System.out.println("getFunction functionURL = " + functionURL);
            try {
                final URL url = URI.create(functionURL).toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final Class<HandlerFunction<ServerResponse>> classToLoad = (Class<HandlerFunction<ServerResponse>>) Class.forName(functionConfiguration.getName(), true, classLoader);
                final HandlerFunction<ServerResponse> handlerFunction = classToLoad.getDeclaredConstructor().newInstance();
                functionMap.put(functionURL, handlerFunction);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return functionMap.get(functionURL);
    }

    private Flux<BiConsumer<ServerRequest, ServerResponse>> getGlobalPostFilters() {
        return Flux.from(subscriber -> subscribePostFilter(configuration.getGlobalConfiguration(), subscriber));
    }

    private Flux<BiConsumer<ServerRequest, ServerResponse>> getEnvironmentPostFilters(String environmentName) {
        if (configuration.getEnvironmentConfigurations().containsKey(environmentName)) {
            return Flux.from(subscriber -> subscribePostFilter(configuration.getEnvironmentConfigurations().get(environmentName), subscriber));
        } else {
            return Flux.empty();
        }
    }

    private Flux<BiConsumer<ServerRequest, ServerResponse>> getTenantPostFilters(String environmentName, String tenantId) {
        if (configuration.getEnvironmentConfigurations().containsKey(environmentName) &&
                configuration.getEnvironmentConfigurations().get(environmentName).getTenantConfigurations().containsKey(tenantId)) {
            final EnvironmentConfiguration environmentConfiguration = configuration.getEnvironmentConfigurations().get(environmentName);
            final TenantConfiguration tenantConfiguration = environmentConfiguration.getTenantConfigurations().get(tenantId);
            return Flux.from(subscriber -> subscribePostFilter(tenantConfiguration, subscriber));
        } else {
            return Flux.empty();
        }
    }

    private void subscribePostFilter(FilterConfiguration filterConfiguration, Subscriber<? super BiConsumer<ServerRequest, ServerResponse>> subscriber) {
        filterConfiguration.getPost()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .forEach(subscriber::onNext);
    }

    private BiConsumer<ServerRequest, ServerResponse> getPostFilter(String codeServerURL, PostFilter postFilter) {
        String filterURL = String.format("%s/%s-%s.jar", codeServerURL, postFilter.getName(), postFilter.getVersion());
        if (!postFilterMap.containsKey(filterURL)) {
            System.out.println("getPostFilter filterURL = " + filterURL);
            try {
                final URL url = URI.create(filterURL).toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final Class<BiConsumer<ServerRequest, ServerResponse>> classToLoad = (Class<BiConsumer<ServerRequest, ServerResponse>>) Class.forName(postFilter.getName(), true, classLoader);
                final BiConsumer<ServerRequest, ServerResponse> filter = classToLoad.getDeclaredConstructor().newInstance();
                postFilterMap.put(filterURL, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return postFilterMap.get(filterURL);
    }

    private Mono<ServerResponse> getErrorResponse(Throwable t, ServerRequest request) {
        final HttpStatus httpStatus = request.attribute("RESPONSE_HTTP_STATUS")
                .map(code -> HttpStatus.valueOf(String.valueOf(code)))
                .orElse(HttpStatus.INTERNAL_SERVER_ERROR);
        final ServerResponse.BodyBuilder bodyBuilder = ServerResponse.status(httpStatus);
        addErrorHeaders(t, bodyBuilder);
        return request.attribute("RESPONSE_MESSAGE")
                .map(o -> bodyBuilder.body(BodyInserters.fromValue(o)))
                .orElse(bodyBuilder.build());
    }

    private void addErrorHeaders(Throwable t, ServerResponse.BodyBuilder bodyBuilder) {
        StringBuilder errorTypes = new StringBuilder();
        errorTypes.append(",").append(t.getClass().getSimpleName());
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(t.getMessage());
        Throwable cause = t.getCause();
        while (Objects.nonNull(cause)) {
            errorMessage.append(",").append(cause.getMessage());
            errorTypes.append(",").append(cause.getClass().getSimpleName());
            cause = cause.getCause();
        }
        bodyBuilder.header("X-A-Error-Type", errorTypes.toString());
        bodyBuilder.header("X-A-Error-Message", errorMessage.toString());
    }


}
