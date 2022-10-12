package io.archura.platform.internal.configuration;

import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.cache.HashCacheRedis;
import io.archura.platform.internal.pubsub.PublishListener;
import io.archura.platform.internal.pubsub.Subscriber;
import io.archura.platform.internal.pubsub.SubscriberRedis;
import io.archura.platform.internal.stream.CacheStream;
import io.archura.platform.internal.stream.CacheStreamRedis;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class CacheConfiguration {

    private final String cacheUrl;
    private RedisCommands<String, String> redisCommands;
    private Subscriber subscriber;
    private PublishListener publishListener;

    public CacheConfiguration(final String cacheUrl) {
        this.cacheUrl = cacheUrl;
    }


    public void createConnections(final ExecutorService executorService, final FilterFunctionExecutor filterFunctionExecutor) {
        final RedisClient redisClient = RedisClient.create(cacheUrl);
        this.createStreamCommands(redisClient);
        this.createPubSubCommands(redisClient, executorService, filterFunctionExecutor);
    }

    private void createStreamCommands(final RedisClient redisClient) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        this.redisCommands = connection.sync();
    }

    private void createPubSubCommands(
            final RedisClient redisClient,
            final ExecutorService executorService,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        final StatefulRedisPubSubConnection<String, String> pubSub = redisClient.connectPubSub();
        final RedisPubSubCommands<String, String> pubSubCommands = pubSub.sync();
        final SubscriberRedis pubSubRedis = new SubscriberRedis(pubSubCommands);
        this.publishListener = new PublishListener(executorService, filterFunctionExecutor);
        pubSub.addListener(this.publishListener);
        this.subscriber = pubSubRedis;
    }


    public HashCache<String, String> getHashCache() {
        return new HashCacheRedis(redisCommands);
    }

    public CacheStream<String, Map<String, String>> getCacheStream() {
        return new CacheStreamRedis(redisCommands);
    }

    public Subscriber getSubscriber() {
        return this.subscriber;
    }

    public PublishListener getPublishListener() {
        return publishListener;
    }
}

