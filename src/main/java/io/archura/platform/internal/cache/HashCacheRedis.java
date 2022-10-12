package io.archura.platform.internal.cache;

import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;

public class HashCacheRedis implements HashCache<String, String> {
    private final RedisCommands<String, String> redisCommands;

    public HashCacheRedis(final RedisCommands<String, String> redisCommands) {
        this.redisCommands = redisCommands;
    }

    @Override
    public boolean set(String tenantKey, String key, String value) {
        return redisCommands.hset(tenantKey, key, value);
    }

    @Override
    public String get(String tenantKey, String key) {
        return redisCommands.hget(tenantKey, key);
    }

    @Override
    public long del(String tenantKey, String[] keys) {
        return redisCommands.hdel(tenantKey, keys);
    }

    @Override
    public boolean exists(String tenantKey, String key) {
        return redisCommands.hexists(tenantKey, key);
    }

    @Override
    public List<String> keys(String tenantKey) {
        return redisCommands.hkeys(tenantKey);
    }

    @Override
    public List<String> vals(String tenantKey) {
        return redisCommands.hvals(tenantKey);
    }

    @Override
    public long len(String tenantKey) {
        return redisCommands.hlen(tenantKey);
    }
}
