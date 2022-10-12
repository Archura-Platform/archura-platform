package io.archura.platform.internal.configuration;

import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.cache.HashCacheRedis;
import io.archura.platform.internal.pubsub.PubSub;
import io.archura.platform.internal.pubsub.PubSubListener;
import io.archura.platform.internal.pubsub.PubSubRedis;
import io.archura.platform.internal.stream.CacheStream;
import io.archura.platform.internal.stream.CacheStreamRedis;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

import java.util.Map;

public class CacheConfiguration {

    private final String cacheUrl;
    private RedisCommands<String, String> redisCommands;
    private RedisPubSubCommands<String, String> pubSubCommands;

    public CacheConfiguration(final String cacheUrl) {
        this.cacheUrl = cacheUrl;
    }

    public void createConnections() {
        final RedisClient redisClient = RedisClient.create(cacheUrl);
        this.createStreamCommands(redisClient);
        this.createPubSubCommands(redisClient);
    }

    private void createStreamCommands(final RedisClient redisClient) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        this.redisCommands = connection.sync();
    }

    private void createPubSubCommands(final RedisClient redisClient) {
        final StatefulRedisPubSubConnection<String, String> pubSub = redisClient.connectPubSub();
        pubSub.addListener(new PubSubListener());
        this.pubSubCommands = pubSub.sync();
    }

    public HashCache<String, String> getHashCache() {
        return new HashCacheRedis(redisCommands);
    }

    public CacheStream<String, Map<String, String>> getCacheStream() {
        return new CacheStreamRedis(redisCommands);
    }

    public PubSub<String, String> getPubSub() {
        return new PubSubRedis(pubSubCommands);
    }
}

