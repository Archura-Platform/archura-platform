package io.archura.platform.internal.logging;


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static java.util.Objects.isNull;

/**
 * Http logger sends the log messages to the remote server.
 */
public class HttpLogWriter implements LogWriter {

    private final String environment;
    private final Object tenantId;
    private final String token;
    private final LogWriter logWriter;
    private final URI uri;
    private final HttpClient httpClient;

    public HttpLogWriter(String environment, Object tenantId, String token, LogWriter defaultLogWriter, URI uri, HttpClient httpClient) {
        this.environment = environment;
        this.tenantId = tenantId;
        this.token = token;
        this.logWriter = defaultLogWriter;
        this.uri = uri;
        this.httpClient = httpClient;
    }

    /**
     * Writes the message on a new line using stream.
     *
     * @param message log message.
     */
    @Override
    public void log(final String logLevel, final String message) {
        final HttpRequest httpRequest = HttpRequest.newBuilder(uri)
                .header("X-A-LogLevel", logLevel)
                .header("X-A-Environment", environment)
                .header("X-A-TenantId", String.valueOf(tenantId))
                .header("X-A-LogToken", String.valueOf(token))
                .POST(HttpRequest.BodyPublishers.ofString(message))
                .build();
        httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                .thenAcceptAsync(voidHttpResponse -> {
                    if (isNull(voidHttpResponse)) {
                        logWriter.log(logLevel, String.format("[REMOTE_SERVER_ERROR][NO_RESPONSE]%s", message));
                    } else if (voidHttpResponse.statusCode() != 201) {
                        logWriter.log(logLevel, String.format("[REMOTE_SERVER_ERROR][COULD_NOT_WRITE]%s", message));
                    }
                });
    }

}