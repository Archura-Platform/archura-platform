package io.archura.platform.internal.tracer;

import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.mapper.Mapper;
import io.archura.platform.api.tracer.Tracer;
import io.archura.platform.internal.logging.DefaultLogger;
import jdk.internal.reflect.Reflection;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;

public class RemoteTracer implements Tracer {

    static {
        Reflection.registerFieldsToFilter(DefaultLogger.class, Set.of("httpClient", "mapper", "logger", "builder"));
    }

    private final HttpClient httpClient;
    private final Mapper mapper;
    private final Logger logger;
    private final HttpRequest.Builder builder;

    public RemoteTracer(
            final String traceServerUrl,
            final HttpClient httpClient,
            final Mapper mapper,
            final Logger logger
    ) {
        this.builder = HttpRequest.newBuilder(URI.create(traceServerUrl));
        this.httpClient = httpClient;
        this.mapper = mapper;
        this.logger = logger;
    }

    @Override
    public void trace(final Map<String, Object> attributes) {
        try {
            final String traceBody = mapper.writeValueAsString(attributes);
            final HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.ofString(traceBody);
            final HttpRequest httpRequest = builder.POST(bodyPublisher).build();
            final CompletableFuture<HttpResponse<Void>> response = httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding());
            response.thenAcceptAsync(voidHttpResponse -> {
                if (isNull(voidHttpResponse)) {
                    logger.error("No response from trace server, could not send trace attributes: %s", attributes);
                } else if (voidHttpResponse.statusCode() != 201) {
                    logger.error("Trace server could not write trace attributes: %s", attributes);
                }
            });
        } catch (Exception e) {
            logger.error("Got exception while preparing/send trace attributes: %s", attributes);
        }
    }
}
