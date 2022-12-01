package io.archura.platform.internal.context;

import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.stream.CacheStream;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class ContextSupport {
    HashCache<String, String> hashCache;
    CacheStream<String, Map<String, String>> cacheStream;
    MessagePublisher messagePublisher;
}
