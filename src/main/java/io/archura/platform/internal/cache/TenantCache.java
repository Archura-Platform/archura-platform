package io.archura.platform.internal.cache;

import io.archura.platform.api.cache.Cache;
import jdk.internal.reflect.Reflection;

import java.util.List;
import java.util.Set;

public class TenantCache implements Cache {

    static {
        Reflection.registerFieldsToFilter(TenantCache.class, Set.of("tenantKey", "hashCache"));
    }

    private final String tenantKey;
    private final HashCache<String, String> hashCache;

    public TenantCache(final String tenantKey, final HashCache<String, String> hashCache) {
        this.tenantKey = tenantKey;
        this.hashCache = hashCache;
    }

    @Override
    public boolean set(String key, String value) {
        return hashCache.set(tenantKey, key, value);
    }

    @Override
    public String get(String key) {
        return hashCache.get(tenantKey, key);
    }

    @Override
    public long del(String... keys) {
        return hashCache.del(tenantKey, keys);
    }

    @Override
    public boolean exists(String key) {
        return hashCache.exists(tenantKey, key);
    }

    @Override
    public List<String> keys() {
        return hashCache.keys(tenantKey);
    }

    @Override
    public List<String> values() {
        return hashCache.vals(tenantKey);
    }

    @Override
    public long length() {
        return hashCache.len(tenantKey);
    }
}
