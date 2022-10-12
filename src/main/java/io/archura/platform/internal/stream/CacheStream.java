package io.archura.platform.internal.stream;

import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;

import java.util.List;

public interface CacheStream<I, M> {
    I send(String streamKey, M message);

    String createGroup(XReadArgs.StreamOffset<I> streamOffset, String group, XGroupCreateArgs args);

    Long clientId();

    List<StreamMessage<I, I>> readMessageFromGroup(Consumer<I> from, XReadArgs.StreamOffset<I> lastConsumed);

    void acknowledge(String topic, String group, I messageIds);
}
