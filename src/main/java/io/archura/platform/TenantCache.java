package io.archura.platform;

import jdk.internal.reflect.Reflection;
import org.springframework.data.redis.core.HashOperations;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TenantCache implements Cache {

    static {
        Reflection.registerFieldsToFilter(TenantCache.class, Set.of("tenantId", "hashOperations"));
    }

    private final String tenantId;
    private final HashOperations<String, String, Map<String, Object>> hashOperations;

    public TenantCache(String tenantId, HashOperations<String, String, Map<String, Object>> hashOperations) {
        this.tenantId = tenantId;
        this.hashOperations = hashOperations;
    }

    @Override
    public Map<String, Object> get(String hashKey) {
        return hashOperations.get(tenantId, hashKey);
    }

    @Override
    public List<Map<String, Object>> multiGet(Collection<String> hashKeys) {
        return hashOperations.multiGet(tenantId, hashKeys);
    }

    @Override
    public void put(String hashKey, Map<String, Object> value) {
        hashOperations.put(tenantId, hashKey, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Map<String, Object>> map) {
        hashOperations.putAll(tenantId, map);
    }

    @Override
    public Boolean putIfAbsent(String hashKey, Map<String, Object> value) {
        return hashOperations.putIfAbsent(tenantId, hashKey, value);
    }

    @Override
    public Boolean hasKey(String hashKey) {
        return hashOperations.hasKey(tenantId, hashKey);
    }

    @Override
    public Set<String> keys() {
        return hashOperations.keys(tenantId);
    }

    @Override
    public List<Map<String, Object>> values() {
        return hashOperations.values(tenantId);
    }

    @Override
    public Map<String, Map<String, Object>> entries() {
        return hashOperations.entries(tenantId);
    }

    @Override
    public Long size() {
        return hashOperations.size(tenantId);
    }

    @Override
    public void delete(String... hashKeys) {
         hashOperations.delete(tenantId, hashKeys);
    }
}
