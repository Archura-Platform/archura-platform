package io.archura.platform.external;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.http.HttpRequest;
import io.archura.platform.api.http.HttpResponse;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.api.type.functionalcore.HandlerFunction;
import io.archura.platform.api.type.functionalcore.StreamConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

public class FilterFunctionExecutor {

    public void execute(Context context, StreamConsumer streamConsumer, String key, Map<String, String> value) {
        streamConsumer.consume(context, key, value);
    }

    public void execute(Context context, ContextConsumer contextConsumer) {
        contextConsumer.accept(context);
    }

    public HttpRequest execute(HttpRequest request, UnaryOperator<HttpRequest> preFilter) {
        return preFilter.apply(request);
    }

    public HttpResponse execute(HttpRequest request, HandlerFunction<HttpResponse> tenantFunction) {
        return tenantFunction.handle(request);
    }

    public HttpResponse execute(HttpRequest request, HttpResponse response, BiFunction<HttpRequest, HttpResponse, HttpResponse> postFilter) {
        return postFilter.apply(request, response);
    }

    public void execute(Configurable configurable, Map<String, Object> config) {
        configurable.setConfiguration(Collections.unmodifiableMap(config));
    }

}
