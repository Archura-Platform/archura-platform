package io.archura.platform.internal.pubsub;

import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

public class SubscriberRedis implements Subscriber {
    private final RedisPubSubCommands<String, String> pubSubCommands;

    public SubscriberRedis(final RedisPubSubCommands<String, String> pubSubCommands) {
        this.pubSubCommands = pubSubCommands;
    }

    @Override
    public void subscribe(final String environmentTenantKey) {
        pubSubCommands.subscribe(environmentTenantKey);
    }

}
