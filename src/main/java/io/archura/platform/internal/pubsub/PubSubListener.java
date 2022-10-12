package io.archura.platform.internal.pubsub;

import io.lettuce.core.pubsub.RedisPubSubAdapter;

public class PubSubListener extends RedisPubSubAdapter<String, String> {
    @Override
    public void message(final String channel, final String message) {
        super.message(channel, message);
    }
}
