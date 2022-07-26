package io.archura.platform.fc.function;

import io.archura.platform.context.Context;

import java.util.Map;

public interface StreamProducer {
    Map.Entry<byte[], byte[]> produce(Context context);
}
