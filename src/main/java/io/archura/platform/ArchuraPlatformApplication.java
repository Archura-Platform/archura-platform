package io.archura.platform;

import io.archura.platform.configuration.Configuration;
import io.archura.platform.configuration.EnvironmentConfiguration;
import io.archura.platform.configuration.Function;
import io.archura.platform.configuration.FunctionsConfiguration;
import io.archura.platform.configuration.GlobalConfiguration;
import io.archura.platform.configuration.PostFilter;
import io.archura.platform.configuration.PreFilter;
import io.archura.platform.configuration.TenantConfiguration;
import io.archura.platform.exception.MissingResourceException;
import io.archura.platform.exception.ResourceLoadException;
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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
//                    getGlobalPreFilters()
//                            .doOnNext(serverRequestConsumer -> serverRequestConsumer.accept(request))


                    String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                    getEnvironmentPreFilters(environmentName).forEach(c -> c.accept(request));

                    String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
                    getTenantPreFilters(environmentName, tenantId).forEach(c -> c.accept(request));

                    return request;
                })
                .route(RequestPredicates.all(), request -> {
                    getGlobalPreFilters()
                            .doOnNext(serverRequestConsumer -> serverRequestConsumer.accept(request))


                    final String routeId = request.attribute("ROUTE_ID")
                            .map(String::valueOf)
                            .orElse(CATCH_ALL_KEY);
                    return getTenantFunctions(request)
                            .getOrDefault(routeId, r -> ServerResponse.notFound().build())
                            .handle(request);
                })
                .after((request, response) -> {
                    String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                    String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
                    getTenantPostFilters(environmentName, tenantId).forEach(bc -> bc.accept(request, response));
                    getEnvironmentPostFilters(environmentName).forEach(bc -> bc.accept(request, response));
                    getGlobalPostFilters().forEach(bc -> bc.accept(request, response));
                    return response;
                })
                .onError(Throwable.class, this::getErrorResponse)
                .build();
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

    private Flux<Consumer<ServerRequest>> getGlobalPreFilters() {
//        if (isNull(configuration.getGlobalConfiguration())) {
//            String globalFilterURL = String.format("%s/global/filters.json", configRepositoryUrl);
//            GlobalConfiguration globalConfiguration = getGlobalConfiguration(globalFilterURL);
//            configuration.setGlobalConfiguration(globalConfiguration);
//        }
//        configuration.getGlobalConfiguration()
//                .getPreFilters()
//                .stream()
//                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
//                .collect(Collectors.toList());
        final String globalFiltersUrl = String.format("%s/global/filters.json", configRepositoryUrl);
        return Flux.from(subscriber ->
                WebClient.create()
                        .get()
                        .uri(URI.create(globalFiltersUrl))
                        .header(X_A_HEADER_KEY_PLATFORM_TOKEN, platformToken)
                        .exchangeToMono(clientResponse -> clientResponse.bodyToMono(GlobalConfiguration.class))
                        .subscribe(globalConfiguration -> globalConfiguration.getPreFilters()
                                .stream()
                                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                                .forEach(subscriber::onNext)));
    }

    private GlobalConfiguration getGlobalConfiguration(String globalFiltersUrl) {
        return WebClient.create()
                .get()
                .uri(URI.create(globalFiltersUrl))
                .header(X_A_HEADER_KEY_PLATFORM_TOKEN, platformToken)
                .exchangeToMono(clientResponse -> clientResponse.bodyToMono(GlobalConfiguration.class))
                .block();
    }

    private List<Consumer<ServerRequest>> getEnvironmentPreFilters(String environmentName) {
        if (isNull(configuration.getEnvironmentConfiguration())) {
            String environmentFiltersURL = String.format("%s/environments/%s/filters.json", configRepositoryUrl, environmentName);
            EnvironmentConfiguration environmentConfiguration = getEnvironmentConfiguration(environmentFiltersURL);
            configuration.setEnvironmentConfiguration(environmentConfiguration);
        }
        return configuration.getEnvironmentConfiguration()
                .getPreFilters()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                .collect(Collectors.toList());
    }

    private EnvironmentConfiguration getEnvironmentConfiguration(String url) {
        return WebClient.create()
                .get()
                .uri(URI.create(url))
                .header(X_A_HEADER_KEY_PLATFORM_TOKEN, platformToken)
                .exchangeToMono(clientResponse -> clientResponse.bodyToMono(EnvironmentConfiguration.class))
                .block();
    }

    private List<Consumer<ServerRequest>> getTenantPreFilters(String environmentName, String tenantId) {
        if (isNull(configuration.getTenantConfiguration())) {
            String tenantFiltersURL = String.format("%s/environments/%s/tenants/%s/filters.json", configRepositoryUrl, environmentName, tenantId);
            TenantConfiguration tenantConfiguration = getTenantConfiguration(tenantFiltersURL);
            configuration.setTenantConfiguration(tenantConfiguration);
        }
        return configuration.getTenantConfiguration()
                .getPreFilters()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                .collect(Collectors.toList());
    }

    private TenantConfiguration getTenantConfiguration(String url) {
        return WebClient.create()
                .get()
                .uri(URI.create(url))
                .header(X_A_HEADER_KEY_PLATFORM_TOKEN, platformToken)
                .exchangeToMono(clientResponse -> clientResponse.bodyToMono(TenantConfiguration.class))
                .block();
    }

    private Consumer<ServerRequest> getPreFilter(String codeServerURL, PreFilter preFilter) {
        String filterURL = String.format("%s/%s-%s.jar", codeServerURL, preFilter.getName(), preFilter.getVersion());
        if (!preFilterMap.containsKey(filterURL)) {
            try {
                final File file = new File(filterURL);
                final URL url = file.toURI().toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final String className = getMainClassName(classLoader);
                final Class<Consumer<ServerRequest>> classToLoad = (Class<Consumer<ServerRequest>>) Class.forName(className, true, classLoader);
                final Consumer<ServerRequest> filter = classToLoad.getDeclaredConstructor().newInstance();
                preFilterMap.put(filterURL, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return preFilterMap.get(filterURL);
    }


    private Map<String, HandlerFunction<ServerResponse>> getTenantFunctions(final ServerRequest serverRequest) {
        if (isNull(configuration.getFunctionsConfiguration())) {
            String environmentName = serverRequest.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
            String tenantId = serverRequest.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
            String tenantFunctionsURL = String.format("%s/environments/%s/tenants/%s/functions.json", configRepositoryUrl, environmentName, tenantId);
            FunctionsConfiguration functionsConfiguration = getTenantFunctionsConfiguration(tenantFunctionsURL);
            configuration.setFunctionsConfiguration(functionsConfiguration);
        }
        return configuration.getFunctionsConfiguration()
                .getRouteFunctions()
                .entrySet()
                .stream()
                .map(entry -> {
                    final String routeId = entry.getKey();
                    final Function function = entry.getValue();
                    final HandlerFunction<ServerResponse> handlerFunction = getFunction(codeRepositoryUrl, function);
                    return new AbstractMap.SimpleEntry<>(routeId, handlerFunction);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private FunctionsConfiguration getTenantFunctionsConfiguration(String url) {
        return WebClient.create()
                .get()
                .uri(URI.create(url))
                .header(X_A_HEADER_KEY_PLATFORM_TOKEN, platformToken)
                .exchangeToMono(clientResponse -> clientResponse.bodyToMono(FunctionsConfiguration.class))
                .block();
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getTenantPostFilters(String environmentName, String tenantId) {
        if (isNull(configuration.getTenantConfiguration())) {
            String tenantFiltersURL = String.format("%s/environments/%s/tenants/%s/filters.json", configRepositoryUrl, environmentName, tenantId);
            TenantConfiguration tenantConfiguration = getTenantConfiguration(tenantFiltersURL);
            configuration.setTenantConfiguration(tenantConfiguration);
        }
        return configuration.getTenantConfiguration()
                .getPostFilters()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .collect(Collectors.toList());
    }

    private BiConsumer<ServerRequest, ServerResponse> getPostFilter(String codeServerURL, PostFilter postFilter) {
        String filterURL = String.format("%s/%s-%s.jar", codeServerURL, postFilter.getName(), postFilter.getVersion());
        if (!postFilterMap.containsKey(filterURL)) {
            try {
                final File file = new File(filterURL);
                final URL url = file.toURI().toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final String className = getMainClassName(classLoader);
                final Class<BiConsumer<ServerRequest, ServerResponse>> classToLoad = (Class<BiConsumer<ServerRequest, ServerResponse>>) Class.forName(className, true, classLoader);
                final BiConsumer<ServerRequest, ServerResponse> filter = classToLoad.getDeclaredConstructor().newInstance();
                postFilterMap.put(filterURL, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return postFilterMap.get(filterURL);
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getEnvironmentPostFilters(String environmentName) {
        if (isNull(configuration.getEnvironmentConfiguration())) {
            String environmentFiltersURL = String.format("%s/environments/%s/filters.json", configRepositoryUrl, environmentName);
            EnvironmentConfiguration environmentConfiguration = getEnvironmentConfiguration(environmentFiltersURL);
            configuration.setEnvironmentConfiguration(environmentConfiguration);
        }
        return configuration.getEnvironmentConfiguration()
                .getPostFilters()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .collect(Collectors.toList());
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getGlobalPostFilters() {
        if (isNull(configuration.getGlobalConfiguration())) {
            String globalFilterURL = String.format("%s/global/filters.json", configRepositoryUrl);
            GlobalConfiguration globalConfiguration = getGlobalConfiguration(globalFilterURL);
            configuration.setGlobalConfiguration(globalConfiguration);
        }
        return configuration.getGlobalConfiguration()
                .getPostFilters()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .collect(Collectors.toList());
    }

    private HandlerFunction<ServerResponse> getFunction(String codeServerURL, Function function) {
        String functionURL = String.format("%s/%s-%s.jar", codeServerURL, function.getName(), function.getVersion());
        if (!functionMap.containsKey(functionURL)) {
            try {
                final File file = new File(functionURL);
                final URL url = file.toURI().toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final String className = getMainClassName(classLoader);
                final Class<HandlerFunction<ServerResponse>> classToLoad = (Class<HandlerFunction<ServerResponse>>) Class.forName(className, true, classLoader);
                final HandlerFunction<ServerResponse> handlerFunction = classToLoad.getDeclaredConstructor().newInstance();
                functionMap.put(functionURL, handlerFunction);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return functionMap.get(functionURL);
    }

    private String getMainClassName(final URLClassLoader classLoader) throws IOException {
        final URL resource = classLoader.findResource(MANIFEST_MF);
        if (isNull(resource)) {
            throw new MissingResourceException(String.format("Missing file: %s", MANIFEST_MF));
        }
        try (Scanner scanner = new Scanner(resource.openStream(), StandardCharsets.UTF_8.name())) {
            String manifestContent = scanner.useDelimiter("\\A").next();
            final String[] lines = manifestContent.split("\\n");
            for (String line : lines) {
                final String[] pair = line.split(":");
                if (pair.length == 2 && MAIN_CLASS.equals(pair[0]) && pair[1] != null && !pair[1].trim().isEmpty()) {
                    return pair[1].trim();
                }
            }
        }
        throw new MissingResourceException(String.format("Main class missing from manifest, '%s' entry missing in %s file.", MAIN_CLASS, MANIFEST_MF));
    }

}
