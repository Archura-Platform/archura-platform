package io.archura.platform.external;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.http.HttpServerRequest;
import io.archura.platform.api.http.HttpServerResponse;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.api.type.functionalcore.HandlerFunction;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import io.archura.platform.api.type.functionalcore.SubscriptionConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

public class FilterFunctionExecutor {

    public void execute(Context context, StreamConsumer streamConsumer, String key, Map<String, String> value) {
        streamConsumer.consume(context, key, value);
    }

    public void execute(Context context, SubscriptionConsumer subscriptionConsumer, String channel, String message) {
        subscriptionConsumer.consume(context, channel, message);
    }

    public void execute(Context context, ContextConsumer contextConsumer) {
        contextConsumer.accept(context);
    }

    public HttpServerRequest execute(HttpServerRequest request, UnaryOperator<HttpServerRequest> preFilter) {
        return preFilter.apply(request);
    }

    public HttpServerResponse execute(HttpServerRequest request, HandlerFunction<HttpServerResponse> tenantFunction) {
        return tenantFunction.handle(request);
    }

    public HttpServerResponse execute(HttpServerRequest request, HttpServerResponse response, BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> postFilter) {
        return postFilter.apply(request, response);
    }

    public void execute(Configurable configurable, Map<String, Object> config) {
        configurable.setConfiguration(Collections.unmodifiableMap(config));
    }

}
