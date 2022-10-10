package io.archura.platform.internal.cache;

import io.archura.platform.api.cache.Cache;
import io.lettuce.core.api.sync.RedisCommands;
import jdk.internal.reflect.Reflection;

import java.util.List;
import java.util.Set;

public class TenantCache implements Cache {

    static {
        Reflection.registerFieldsToFilter(TenantCache.class, Set.of("tenantKey", "redisCommands"));
    }

    private final String tenantKey;
    private final RedisCommands<String, String> redisCommands;

    public TenantCache(final String tenantKey, final RedisCommands<String, String> redisCommands) {
        this.tenantKey = tenantKey;
        this.redisCommands = redisCommands;
    }

    @Override
    public boolean set(String key, String value) {
        return redisCommands.hset(tenantKey, key, value);
    }

    @Override
    public String get(String key) {
        return redisCommands.hget(tenantKey, key);
    }

    @Override
    public long del(String... keys) {
        return redisCommands.hdel(tenantKey, keys);
    }

    @Override
    public boolean exists(String key) {
        return redisCommands.hexists(tenantKey, key);
    }

    @Override
    public List<String> keys() {
        return redisCommands.hkeys(tenantKey);
    }

    @Override
    public List<String> values() {
        return redisCommands.hvals(tenantKey);
    }

    @Override
    public long length() {
        return redisCommands.hlen(tenantKey);
    }
}
