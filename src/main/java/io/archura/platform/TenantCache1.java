//package io.archura.platform;
//
//import jdk.internal.reflect.Reflection;
//import org.springframework.data.redis.core.ReactiveHashOperations;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public class TenantCache1 implements Cache {
//
//    static {
//        Reflection.registerFieldsToFilter(TenantCache1.class, Set.of("tenantId", "hashOperations"));
//    }
//
//    private final String tenantId;
//    private final ReactiveHashOperations<String, String, Map<String, Object>> hashOperations;
//
//    public TenantCache1(String tenantId, ReactiveHashOperations<String, String, Map<String, Object>> hashOperations) {
//        this.tenantId = tenantId;
//        this.hashOperations = hashOperations;
//    }
//
//    @Override
//    public Mono<Map<String, Object>> get(String hashKey) {
//        return hashOperations.get(tenantId, hashKey);
//    }
//
//    @Override
//    public Mono<List<Map<String, Object>>> multiGet(Collection<String> hashKeys) {
//        return hashOperations.multiGet(tenantId, hashKeys);
//    }
//
//    @Override
//    public Mono<Boolean> put(String hashKey, Map<String, Object> value) {
//        return hashOperations.put(tenantId, hashKey, value);
//    }
//
//    @Override
//    public Mono<Boolean> putAll(Map<? extends String, ? extends Map<String, Object>> map) {
//        return hashOperations.putAll(tenantId, map);
//    }
//
//    @Override
//    public Mono<Boolean> putIfAbsent(String hashKey, Map<String, Object> value) {
//        return hashOperations.putIfAbsent(tenantId, hashKey, value);
//    }
//
//    @Override
//    public Mono<Boolean> hasKey(String hashKey) {
//        return hashOperations.hasKey(tenantId, hashKey);
//    }
//
//    @Override
//    public Flux<String> keys() {
//        return hashOperations.keys(tenantId);
//    }
//
//    @Override
//    public Flux<Map<String, Object>> values() {
//        return hashOperations.values(tenantId);
//    }
//
//    @Override
//    public Flux<Map.Entry<String, Map<String, Object>>> entries() {
//        return hashOperations.entries(tenantId);
//    }
//
//    @Override
//    public Mono<Long> size() {
//        return hashOperations.size(tenantId);
//    }
//
//    @Override
//    public Mono<Long> remove(String... hashKeys) {
//        return hashOperations.remove(tenantId, hashKeys);
//    }
//
//    @Override
//    public Mono<Boolean> delete() {
//        return hashOperations.delete(tenantId);
//    }
//}
