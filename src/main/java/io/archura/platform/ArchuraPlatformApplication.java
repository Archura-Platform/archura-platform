package io.archura.platform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.HandlerFunction;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.isNull;

@SpringBootApplication
public class ArchuraPlatformApplication {

    public static final String MANIFEST_MF = "META-INF/MANIFEST.MF";
    private final Map<String, HandlerFunction<ServerResponse>> functionMap = new ConcurrentHashMap<>();
    private String defaultClassName = "Function";

    public static void main(String[] args) {
        SpringApplication.run(ArchuraPlatformApplication.class, args);
    }

    /**
     * Entry point for all requests.
     *
     * @return Router function that handles all the requests.
     */
    @Bean
    public RouterFunction<ServerResponse> singleRouteNoFilter(ReactiveHashOperations<String, String, Map<String, Object>> hashOperations) {
        return RouterFunctions
                .route()
                .route(RequestPredicates.GET("/test"), request -> {
                    final String tenantId = "tenantId001";
                    request.attributes().put(Cache.class.getSimpleName(), new TenantCache(tenantId, hashOperations));
                    try {
                        final HandlerFunction<ServerResponse> function = getFunctionForPath(
                                "/tmp/archura-platform-function-example-0.0.1.jar"
                        );
                        final Mono<ServerResponse> handle = function.handle(request);
                        return handle.timeout(Duration.ofSeconds(30));
                    } catch (Exception e) {
                        return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).body(BodyInserters.fromValue(e.getMessage()));
                    }
                })
                .route(RequestPredicates.all(), serverRequest -> ServerResponse.ok().build())
                .build();
    }

    private HandlerFunction<ServerResponse> getFunctionForPath(String functionURL)
            throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        if (!functionMap.containsKey(functionURL)) {
            final File file = new File(functionURL);
            final URL url = file.toURI().toURL();
            final URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
            final String className = getMainClassName(classLoader);
            final Class<HandlerFunction<ServerResponse>> classToLoad = (Class<HandlerFunction<ServerResponse>>) Class.forName(className, true, classLoader);
            final HandlerFunction<ServerResponse> function = classToLoad.getDeclaredConstructor().newInstance();
            functionMap.put(functionURL, function);
            System.out.println("functionURL = " + functionURL);
        }
        return functionMap.get(functionURL);
    }

    private String getMainClassName(final URLClassLoader classLoader) throws IOException {
        final URL resource = classLoader.findResource(MANIFEST_MF);
        if (isNull(resource)) {
            return defaultClassName;
        }
        try (Scanner scanner = new Scanner(resource.openStream(), StandardCharsets.UTF_8.name())) {
            String manifestContent = scanner.useDelimiter("\\A").next();
            final String[] lines = manifestContent.split("\\n");
            for (String line : lines) {
                final String[] pair = line.split(":");
                if (pair.length == 2 && "Main-Class".equals(pair[0]) && pair[1] != null && !pair[1].trim().isEmpty()) {
                    return pair[1].trim();
                }
            }
        }
        return defaultClassName;
    }

}
