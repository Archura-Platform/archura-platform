package io.archura.platform.internal.publish;

import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

public class RedisMessagePublisher implements MessagePublisher {
    private final RedisPubSubCommands<String, String> pubSubCommands;

    public RedisMessagePublisher(final RedisPubSubCommands<String, String> pubSubCommands) {
        this.pubSubCommands = pubSubCommands;
    }

    @Override
    public void publish(String channel, String message) {
        pubSubCommands.publish(channel, message);
    }
}
