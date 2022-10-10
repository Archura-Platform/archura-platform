package io.archura.platform.internal.configuration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class CacheConfiguration {

    private final String redisUrl;
    private RedisCommands<String, String> redisCommands;

    public CacheConfiguration(final String redisUrl) {
        this.redisUrl = redisUrl;
    }

    public void createRedisConnectionFactory() {
        RedisClient redisClient = RedisClient.create(redisUrl);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        this.redisCommands = connection.sync();
    }

    public RedisCommands<String, String> getRedisCommands() {
        return redisCommands;
    }
}

