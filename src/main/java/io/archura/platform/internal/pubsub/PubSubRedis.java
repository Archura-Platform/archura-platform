package io.archura.platform.internal.pubsub;

import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

public class PubSubRedis implements PubSub<String, String> {
    private final RedisPubSubCommands<String, String> pubSubCommands;

    public PubSubRedis(final RedisPubSubCommands<String, String> pubSubCommands) {
        this.pubSubCommands = pubSubCommands;
    }
}
