package io.archura.platform.cache;

import jdk.internal.reflect.Reflection;
import org.springframework.data.redis.core.HashOperations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TenantCache implements Cache {

    static {
        Reflection.registerFieldsToFilter(TenantCache.class, Set.of("tenantKey", "hashOperations"));
    }

    private final String tenantKey;
    private final HashOperations<String, String, Map<String, Object>> hashOperations;

    public TenantCache(String tenantKey, HashOperations<String, String, Map<String, Object>> hashOperations) {
        this.tenantKey = tenantKey;
        this.hashOperations = hashOperations;
    }

    @Override
    public Map<String, Object> get(String hashKey) {
        return hashOperations.get(tenantKey, hashKey);
    }

    @Override
    public List<Map<String, Object>> multiGet(Collection<String> hashKeys) {
        return hashOperations.multiGet(tenantKey, hashKeys);
    }

    @Override
    public void put(String hashKey, Map<String, Object> value) {
        hashOperations.put(tenantKey, hashKey, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Map<String, Object>> map) {
        hashOperations.putAll(tenantKey, map);
    }

    @Override
    public Boolean putIfAbsent(String hashKey, Map<String, Object> value) {
        return hashOperations.putIfAbsent(tenantKey, hashKey, value);
    }

    @Override
    public Boolean hasKey(String hashKey) {
        return hashOperations.hasKey(tenantKey, hashKey);
    }

    @Override
    public Set<String> keys() {
        return hashOperations.keys(tenantKey);
    }

    @Override
    public List<Map<String, Object>> values() {
        return hashOperations.values(tenantKey);
    }

    @Override
    public Map<String, Map<String, Object>> entries() {
        return hashOperations.entries(tenantKey);
    }

    @Override
    public Long size() {
        return hashOperations.size(tenantKey);
    }

    @Override
    public void delete(String... hashKeys) {
        hashOperations.delete(tenantKey, hashKeys);
    }
}
