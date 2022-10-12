package io.archura.platform.internal.cache;

import java.util.List;

public interface HashCache<K, V> {

    boolean set(String tenantKey, K key, V value);

    String get(String tenantKey, K key);

    long del(String tenantKey, K[] keys);

    boolean exists(String tenantKey, K key);

    List<K> keys(String tenantKey);

    List<V> vals(String tenantKey);

    long len(String tenantKey);
}
