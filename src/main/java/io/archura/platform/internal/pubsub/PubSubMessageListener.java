package io.archura.platform.internal.pubsub;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.type.functionalcore.SubscriptionConsumer;
import io.archura.platform.external.FilterFunctionExecutor;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.isNull;

public class PubSubMessageListener extends RedisPubSubAdapter<String, String> {

    private final ExecutorService executorService;
    private final FilterFunctionExecutor filterFunctionExecutor;
    private final Map<String, List<ConsumerContext>> channelSubscribers = new HashMap<>();

    public PubSubMessageListener(final ExecutorService executorService, final FilterFunctionExecutor filterFunctionExecutor) {
        this.executorService = executorService;
        this.filterFunctionExecutor = filterFunctionExecutor;
    }

    public void register(final String channel, final Context context, final SubscriptionConsumer subscriptionConsumer) {
        if (isNull(this.channelSubscribers.get(channel))) {
            this.channelSubscribers.put(channel, new ArrayList<>());
        }
        final List<ConsumerContext> consumerContexts = this.channelSubscribers.get(channel);
        consumerContexts.add(new ConsumerContext(context, subscriptionConsumer));
    }

    @Override
    public void message(String channel, String message) {
        if (this.channelSubscribers.containsKey(channel)) {
            final List<ConsumerContext> consumerContexts = this.channelSubscribers.get(channel);
            for (ConsumerContext consumerContext : consumerContexts) {
                executorService.submit(() -> filterFunctionExecutor.execute(consumerContext.getContext(), consumerContext.getConsumer(), channel, message));
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static class ConsumerContext {
        private Context context;
        private SubscriptionConsumer consumer;
    }
}
