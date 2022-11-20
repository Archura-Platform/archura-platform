package io.archura.platform.external;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.http.HttpServerRequest;
import io.archura.platform.api.http.HttpServerResponse;
import io.archura.platform.api.type.Configurable;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.api.type.functionalcore.LightStreamConsumer;
import io.archura.platform.api.type.functionalcore.SubscriptionConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class FilterFunctionExecutor {

    public void execute(Context context, LightStreamConsumer lightStreamConsumer, String key, Map<String, String> value) {
        lightStreamConsumer.consume(context, key, value);
    }

    public void execute(Context context, SubscriptionConsumer subscriptionConsumer, String channel, String message) {
        subscriptionConsumer.consume(context, channel, message);
    }

    public void execute(Context context, ContextConsumer contextConsumer) {
        contextConsumer.accept(context);
    }

    public void execute(HttpServerRequest request, Consumer<HttpServerRequest> preFilter) {
        preFilter.accept(request);
    }

    public HttpServerResponse execute(HttpServerRequest request, Function<HttpServerRequest, HttpServerResponse> tenantFunction) {
        return tenantFunction.apply(request);
    }

    public void execute(HttpServerRequest request, HttpServerResponse response, BiConsumer<HttpServerRequest, HttpServerResponse> postFilter) {
        postFilter.accept(request, response);
    }

    public void execute(Configurable configurable, Map<String, Object> config) {
        configurable.setConfiguration(Collections.unmodifiableMap(config));
    }

}
