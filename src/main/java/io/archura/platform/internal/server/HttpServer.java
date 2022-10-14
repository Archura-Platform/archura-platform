package io.archura.platform.internal.server;

import com.sun.net.httpserver.HttpContext;
import io.archura.platform.api.http.HttpServerRequest;
import io.archura.platform.api.http.HttpServerResponse;
import io.archura.platform.api.http.HttpStatusCode;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.RequestHandler;
import io.archura.platform.internal.configuration.GlobalConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class HttpServer implements Server {

    private com.sun.net.httpserver.HttpServer localhost;

    @Override
    public void start(
            final RequestHandler requestHandler,
            final ExecutorService executorService,
            final Assets assets
    ) throws IOException {
        final GlobalConfiguration globalConfiguration = GlobalConfiguration.getInstance();
        final InetSocketAddress serverAddress = new InetSocketAddress(globalConfiguration.getConfig().getHostname(), globalConfiguration.getConfig().getPort());
        localhost = com.sun.net.httpserver.HttpServer.create(serverAddress, globalConfiguration.getConfig().getBacklog());
        localhost.setExecutor(executorService);
        final HttpContext context = localhost.createContext("/", exchange -> {
            try {
                final Map<String, List<String>> headers = exchange.getResponseHeaders()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                final HttpServerRequest httpRequest = HttpServerRequest.builder()
                        .requestURI(exchange.getRequestURI())
                        .requestMethod(exchange.getRequestMethod())
                        .requestHeaders(headers)
                        .requestBody(exchange.getRequestBody())
                        .attributes(exchange.getHttpContext().getAttributes())
                        .remoteAddress(exchange.getRemoteAddress())
                        .build();
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
        context.getFilters().add(new RequestFilter(globalConfiguration.getConfig().getRequestTimeout()));
        final Logger logger = assets.getLogger(Collections.emptyMap());
        logger.info("HTTP server started on: %s:%s", localhost.getAddress().getHostName(), localhost.getAddress().getPort());
        localhost.start();
    }

    @Override
    public void stop() {
        if (nonNull(localhost)) {
            localhost.stop(0);
        }
    }
}
