package io.archura.platform.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Cache {

    Map<String, Object> get(String key);

    List<Map<String, Object>> multiGet(Collection<String> keys);

    void put(String key, Map<String, Object> value);

    void putAll(Map<? extends String, ? extends Map<String, Object>> map);

    Boolean putIfAbsent(String key, Map<String, Object> value);

    Boolean hasKey(String key);

    Set<String> keys();

    List<Map<String, Object>> values();

    Map<String, Map<String, Object>> entries();

    Long size();

    void delete(String... hashKeys);
}
