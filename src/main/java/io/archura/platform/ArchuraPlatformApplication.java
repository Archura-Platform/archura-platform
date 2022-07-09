package io.archura.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.configuration.Configuration;
import io.archura.platform.configuration.EnvironmentFiltersConfiguration;
import io.archura.platform.configuration.Function;
import io.archura.platform.configuration.FunctionsConfiguration;
import io.archura.platform.configuration.GlobalFiltersConfiguration;
import io.archura.platform.configuration.PostFilter;
import io.archura.platform.configuration.PreFilter;
import io.archura.platform.configuration.TenantFiltersConfiguration;
import io.archura.platform.exception.ConfigurationException;
import io.archura.platform.exception.MissingResourceException;
import io.archura.platform.exception.ResourceLoadException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.RequestPredicates;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

@SpringBootApplication
public class ArchuraPlatformApplication {

    private static final String MAIN_CLASS = "Main-Class";
    private static final String DEFAULT_ENVIRONMENT = "default";
    private static final String DEFAULT_TENANT_ID = "default";
    private static final String CATCH_ALL_KEY = "*";
    private static final ExecutorService HTTP_CLIENT_EXECUTOR = Executors.newCachedThreadPool();

    @Value("${config.repository.url:http://config-service/}")
    private String configRepositoryUrl;

    @Value("${code.repository.url:http://code-service/}")
    private String codeRepositoryUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Configuration configuration = new Configuration();

    private static final String MANIFEST_MF = "META-INF/MANIFEST.MF";
    private final Map<String, HandlerFunction<ServerResponse>> functionMap = new ConcurrentHashMap<>();

    private final Map<String, Consumer<ServerRequest>> preFilterMap = new ConcurrentHashMap<>();
    private final Map<String, BiConsumer<ServerRequest, ServerResponse>> postFilterMap = new ConcurrentHashMap<>();


    public static void main(String[] args) {
        SpringApplication.run(ArchuraPlatformApplication.class, args);
    }

    @Bean
    public RouterFunction<ServerResponse> routes() {
        return RouterFunctions.route()
                .route(RequestPredicates.all(), request -> {
                    try {
                        getGlobalPreFilters().forEach(c -> c.accept(request));

                        String environmentName = request.attribute("ENVIRONMENT").map(String::valueOf).orElse(DEFAULT_ENVIRONMENT);
                        getEnvironmentPreFilters(environmentName).forEach(c -> c.accept(request));

                        String tenantId = request.attribute("TENANT_ID").map(String::valueOf).orElse(DEFAULT_TENANT_ID);
                        getTenantPreFilters(environmentName, tenantId).forEach(c -> c.accept(request));

                        final String routeId = request.attribute("ROUTE_ID").map(String::valueOf).orElse(CATCH_ALL_KEY);
                        final ServerResponse response = getTenantFunctions(request)
                                .getOrDefault(routeId, r -> ServerResponse.notFound().build())
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
        if (isNull(configuration.getGlobalFiltersConfiguration())) {
            String globalFilterURL = String.format("%s/global/filters.json", configRepositoryUrl);
            GlobalFiltersConfiguration globalFiltersConfiguration = getGlobalConfiguration(globalFilterURL);
            configuration.setGlobalFiltersConfiguration(globalFiltersConfiguration);
        }
        return configuration.getGlobalFiltersConfiguration()
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                .toList();
    }

    private GlobalFiltersConfiguration getGlobalConfiguration(String url) {
        return getConfiguration(url, GlobalFiltersConfiguration.class);
    }

    private List<Consumer<ServerRequest>> getEnvironmentPreFilters(String environmentName) {
        if (isNull(configuration.getEnvironmentFiltersConfiguration())) {
            String environmentFiltersURL = String.format("%s/environments/%s/filters.json", configRepositoryUrl, environmentName);
            EnvironmentFiltersConfiguration environmentFiltersConfiguration = getEnvironmentConfiguration(environmentFiltersURL);
            configuration.setEnvironmentFiltersConfiguration(environmentFiltersConfiguration);
        }
        return configuration.getEnvironmentFiltersConfiguration()
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                .toList();
    }

    private EnvironmentFiltersConfiguration getEnvironmentConfiguration(String url) {
        return getConfiguration(url, EnvironmentFiltersConfiguration.class);
    }

    private List<Consumer<ServerRequest>> getTenantPreFilters(String environmentName, String tenantId) {
        if (isNull(configuration.getTenantFiltersConfiguration())) {
            String tenantFiltersURL = String.format("%s/environments/%s/tenants/%s/filters.json", configRepositoryUrl, environmentName, tenantId);
            TenantFiltersConfiguration tenantFiltersConfiguration = getTenantConfiguration(tenantFiltersURL);
            configuration.setTenantFiltersConfiguration(tenantFiltersConfiguration);
        }
        return configuration.getTenantFiltersConfiguration()
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter))
                .toList();
    }

    private TenantFiltersConfiguration getTenantConfiguration(String url) {
        return getConfiguration(url, TenantFiltersConfiguration.class);
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
        return getConfiguration(url, FunctionsConfiguration.class);
    }

    private HandlerFunction<ServerResponse> getFunction(String codeServerURL, Function function) {
        String functionURL = String.format("%s/%s-%s.jar", codeServerURL, function.getName(), function.getVersion());
        if (!functionMap.containsKey(functionURL)) {
            try {
                final File file = new File(functionURL);
                final URL url = file.toURI().toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final String className = getMainClassName(classLoader);
                @SuppressWarnings("unchecked") final Class<HandlerFunction<ServerResponse>> classToLoad =
                        (Class<HandlerFunction<ServerResponse>>) Class.forName(className, true, classLoader);
                final HandlerFunction<ServerResponse> handlerFunction = classToLoad.getDeclaredConstructor().newInstance();
                functionMap.put(functionURL, handlerFunction);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return functionMap.get(functionURL);
    }


    private List<BiConsumer<ServerRequest, ServerResponse>> getTenantPostFilters(String environmentName, String tenantId) {
        if (isNull(configuration.getTenantFiltersConfiguration())) {
            String tenantFiltersURL = String.format("%s/environments/%s/tenants/%s/filters.json", configRepositoryUrl, environmentName, tenantId);
            TenantFiltersConfiguration tenantFiltersConfiguration = getTenantConfiguration(tenantFiltersURL);
            configuration.setTenantFiltersConfiguration(tenantFiltersConfiguration);
        }
        return configuration.getTenantFiltersConfiguration()
                .getPost()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .toList();
    }

    private BiConsumer<ServerRequest, ServerResponse> getPostFilter(String codeServerURL, PostFilter postFilter) {
        String filterURL = String.format("%s/%s-%s.jar", codeServerURL, postFilter.getName(), postFilter.getVersion());
        if (!postFilterMap.containsKey(filterURL)) {
            try {
                final File file = new File(filterURL);
                final URL url = file.toURI().toURL();
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
                final String className = getMainClassName(classLoader);
                @SuppressWarnings("unchecked") final Class<BiConsumer<ServerRequest, ServerResponse>> classToLoad =
                        (Class<BiConsumer<ServerRequest, ServerResponse>>) Class.forName(className, true, classLoader);
                final BiConsumer<ServerRequest, ServerResponse> filter = classToLoad.getDeclaredConstructor().newInstance();
                postFilterMap.put(filterURL, filter);
            } catch (Exception e) {
                throw new ResourceLoadException(e);
            }
        }
        return postFilterMap.get(filterURL);
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getEnvironmentPostFilters(String environmentName) {
        if (isNull(configuration.getEnvironmentFiltersConfiguration())) {
            String environmentFiltersURL = String.format("%s/environments/%s/filters.json", configRepositoryUrl, environmentName);
            EnvironmentFiltersConfiguration environmentFiltersConfiguration = getEnvironmentConfiguration(environmentFiltersURL);
            configuration.setEnvironmentFiltersConfiguration(environmentFiltersConfiguration);
        }
        return configuration.getEnvironmentFiltersConfiguration()
                .getPost()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .toList();
    }

    private List<BiConsumer<ServerRequest, ServerResponse>> getGlobalPostFilters() {
        if (isNull(configuration.getGlobalFiltersConfiguration())) {
            String globalFilterURL = String.format("%s/global/filters.json", configRepositoryUrl);
            GlobalFiltersConfiguration globalFiltersConfiguration = getGlobalConfiguration(globalFilterURL);
            configuration.setGlobalFiltersConfiguration(globalFiltersConfiguration);
        }
        return configuration.getGlobalFiltersConfiguration()
                .getPost()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter))
                .toList();
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
        List<String> errorTypes = new ArrayList<>();
        List<String> errorMessages = new ArrayList<>();
        errorTypes.add(t.getClass().getSimpleName());
        errorMessages.add(t.getMessage());
        Throwable cause = t.getCause();
        while (Objects.nonNull(cause)) {
            errorMessages.add(cause.getMessage());
            errorTypes.add(cause.getClass().getSimpleName());
            cause = cause.getCause();
        }
        bodyBuilder.header("X-A-Error-Type", String.join(", ", errorTypes));
        bodyBuilder.header("X-A-Error-Message", String.join(", ", errorMessages));
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
