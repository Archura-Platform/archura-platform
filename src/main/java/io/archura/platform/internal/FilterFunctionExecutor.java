package io.archura.platform.internal;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

@Component
public class FilterFunctionExecutor {

    public void execute(Context context, StreamConsumer streamConsumer, byte[] key, byte[] value) {
        streamConsumer.consume(context, key, value);
    }

    public void execute(Context context, ContextConsumer contextConsumer) {
        contextConsumer.accept(context);
    }

    public ServerRequest execute(ServerRequest request, UnaryOperator<ServerRequest> preFilter) {
        return preFilter.apply(request);
    }

    public ServerResponse execute(ServerRequest request, HandlerFunction<ServerResponse> tenantFunction) throws Exception {
        return tenantFunction.handle(request);
    }

    public ServerResponse execute(ServerRequest request, ServerResponse response, BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter) {
        return postFilter.apply(request, response);
    }

}
