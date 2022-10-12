package io.archura.platform.internal.publish;

import io.archura.platform.api.publish.Publisher;
import jdk.internal.reflect.Reflection;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@RequiredArgsConstructor
public class TenantPublisher implements Publisher {
    static {
        Reflection.registerFieldsToFilter(TenantPublisher.class, Set.of("tenantKey", "messagePublisher"));
    }

    private final String tenantKey;
    private final MessagePublisher messagePublisher;

    public void publish(final String channel, final String message) {
        final String channelKey = String.format("channel|%s|%s", tenantKey, channel);
        messagePublisher.publish(channelKey, message);
    }
}
