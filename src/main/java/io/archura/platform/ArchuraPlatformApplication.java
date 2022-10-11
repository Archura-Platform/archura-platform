package io.archura.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.archura.platform.api.http.HttpServerResponse;
import io.archura.platform.api.http.HttpStatusCode;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.Initializer;
import io.archura.platform.internal.RequestHandler;
import io.archura.platform.internal.configuration.ApplicationConfiguration;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.http.HttpServerRequestData;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public class ArchuraPlatformApplication {

    public static void main(String[] args) throws Exception {
        final ArchuraPlatformApplication application = new ArchuraPlatformApplication();
        application.start();
    }

    private void start() throws IOException {
        final String configRepositoryUrl = Optional.ofNullable(System.getenv("CONFIG_REPOSITORY_URL")).orElse("http://config-service");
        final ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
        final ThreadFactory threadFactory = applicationConfiguration.threadFactory();
        final ExecutorService executorService = applicationConfiguration.executorService(threadFactory);
        final ObjectMapper objectMapper = applicationConfiguration.objectMapper();
        final HttpClient httpClient = applicationConfiguration.httpClient();
        final FilterFunctionExecutor filterFunctionExecutor = applicationConfiguration.filterFunctionExecutor();
        final Assets assets = applicationConfiguration.assets(objectMapper, httpClient, filterFunctionExecutor);
        final Initializer initializer = applicationConfiguration.initializer(configRepositoryUrl, httpClient, threadFactory, executorService, assets, filterFunctionExecutor);
        final RequestHandler requestHandler = applicationConfiguration.requestHandler(configRepositoryUrl, httpClient, assets, filterFunctionExecutor);
        initializer.initialize();

        runServer(requestHandler, executorService, assets);
    }

    private static void runServer(
            final RequestHandler requestHandler,
            final ExecutorService executorService,
            final Assets assets
    ) throws IOException {
        final GlobalConfiguration globalConfiguration = GlobalConfiguration.getInstance();
        final InetSocketAddress serverAddress = new InetSocketAddress(globalConfiguration.getConfig().getHostname(), globalConfiguration.getConfig().getPort());
        final HttpServer localhost = HttpServer.create(serverAddress, globalConfiguration.getConfig().getBacklog());
        localhost.setExecutor(executorService);
        localhost.createContext("/", exchange -> {
            try {
                final Map<String, List<String>> headers = exchange.getResponseHeaders()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                final HttpServerRequestData httpRequest = new HttpServerRequestData(
                        exchange.getRequestURI(),
                        exchange.getRequestMethod(),
                        headers,
                        exchange.getRequestBody(),
                        exchange.getHttpContext().getAttributes()
                );
                final HttpServerResponse response = requestHandler.handle(httpRequest);
                exchange.getResponseHeaders().putAll(response.getHeaders());
                exchange.sendResponseHeaders(response.getStatus(), response.getBytes().length);
                exchange.getResponseBody().write(response.getBytes());
                exchange.getResponseBody().close();
            } catch (Throwable throwable) {
                final byte[] bytes = throwable.getMessage().getBytes();
                exchange.sendResponseHeaders(HttpStatusCode.HTTP_INTERNAL_ERROR, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.getResponseBody().close();
            }
        });
        final Logger logger = assets.getLogger(Collections.emptyMap());
        logger.info("HTTP server started on: %s:%s", localhost.getAddress().getHostName(), localhost.getAddress().getPort());
        localhost.start();
    }

}
