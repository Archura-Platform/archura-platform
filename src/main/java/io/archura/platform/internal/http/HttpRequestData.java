package io.archura.platform.internal.http;

import io.archura.platform.api.http.HttpRequest;
import io.archura.platform.internal.http.exception.RequestReadException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HttpRequestData implements HttpRequest {

    private final URI requestURI;
    private final String requestMethod;
    private final Map<String, List<String>> headers;
    private final InputStream requestBody;
    private final Map<String, Object> attributes;

    public HttpRequestData(
            final URI requestURI,
            final String requestMethod,
            final Map<String, List<String>> headers,
            final InputStream requestBody,
            final Map<String, Object> attributes
    ) {
        this.requestURI = requestURI;
        this.requestMethod = requestMethod;
        this.headers = headers;
        this.requestBody = requestBody;
        this.attributes = attributes;
    }

    @Override
    public Map<String, List<String>> getRequestHeaders() {
        return headers;
    }

    @Override
    public URI getRequestURI() {
        return requestURI;
    }

    @Override
    public String getRequestMethod() {
        return requestMethod;
    }

    @Override
    public InputStream getRequestStream() {
        return requestBody;
    }

    @Override
    public byte[] getRequestBytes() {
        try {
            return requestBody.readAllBytes();
        } catch (IOException e) {
            throw new RequestReadException(e);
        }
    }

    @Override
    public String getRequestBody() {
        try {
            return new String(requestBody.readAllBytes());
        } catch (IOException e) {
            throw new RequestReadException(e);
        }
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }
}
