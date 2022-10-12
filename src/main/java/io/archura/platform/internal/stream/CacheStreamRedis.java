package io.archura.platform.internal.stream;

import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.Map;

public class CacheStreamRedis implements CacheStream<String, Map<String, String>> {
    private final RedisCommands<String, String> redisCommands;

    public CacheStreamRedis(final RedisCommands<String, String> redisCommands) {
        this.redisCommands = redisCommands;
    }

    @Override
    public String send(String streamKey, Map<String, String> message) {
        return redisCommands.xadd(streamKey, message);
    }

    @Override
    public String createGroup(final XReadArgs.StreamOffset<String> streamOffset, final String group, final XGroupCreateArgs args) {
        return redisCommands.xgroupCreate(streamOffset, group, args);
    }

    @Override
    public List<StreamMessage<String, String>> readMessageFromGroup(Consumer<String> from, XReadArgs.StreamOffset<String> lastConsumed) {
        return redisCommands.xreadgroup(from, lastConsumed);
    }

    @Override
    public void acknowledge(String topic, String group, String messageId) {
        redisCommands.xack(topic, group, messageId);
    }
}
