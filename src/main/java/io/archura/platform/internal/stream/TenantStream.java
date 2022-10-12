package io.archura.platform.internal.stream;

import io.archura.platform.api.stream.LightStream;
import jdk.internal.reflect.Reflection;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class TenantStream implements LightStream {

    static {
        Reflection.registerFieldsToFilter(TenantStream.class, Set.of("tenantKey", "cacheStream"));
    }

    private final String tenantKey;
    private final CacheStream<String, Map<String, String>> cacheStream;

    public String send(final String topicName, final Map<String, String> message) {
        final String streamKey = String.format("%s-%s", tenantKey, topicName);
        return cacheStream.send(streamKey, message);
    }
}
