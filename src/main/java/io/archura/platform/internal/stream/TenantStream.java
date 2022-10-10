package io.archura.platform.internal.stream;

import io.archura.platform.api.stream.LightStream;
import io.lettuce.core.api.sync.RedisCommands;
import jdk.internal.reflect.Reflection;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class TenantStream implements LightStream {

    static {
        Reflection.registerFieldsToFilter(TenantStream.class, Set.of("tenantKey", "redisCommands"));
    }

    private final String tenantKey;
    private final RedisCommands<String, String> redisCommands;

    public String send(final String topicName, final Map<String, String> message) {
        final String streamKey = String.format("%s-%s", tenantKey, topicName);
        return redisCommands.xadd(streamKey, message);
    }

}
