package io.archura.platform.internal.http;

import io.archura.platform.api.http.HttpResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpResponseData implements HttpResponse {

    private byte[] bytes;
    private int statusCode;
    private Map<String, List<String>> responseHeaders = new HashMap<>();

    @Override
    public void setResponseBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public void setResponseHeader(String key, String... values) {
        getResponseHeaders().put(key, List.of(values));
    }

    @Override
    public Map<String, List<String>> getResponseHeaders() {
        return this.responseHeaders;
    }

}
