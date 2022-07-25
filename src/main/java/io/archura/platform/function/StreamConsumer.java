package io.archura.platform.function;

import io.archura.platform.context.Context;

@FunctionalInterface
public interface StreamConsumer {

    void consume(Context context, byte[] key, byte[] value);

}
