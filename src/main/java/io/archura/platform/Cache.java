package io.archura.platform;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface Cache {

    Mono<Map<String, Object>> get(String key);

    Mono<List<Map<String, Object>>> multiGet(Collection<String> keys);

    Mono<Boolean> put(String key, Map<String, Object> value);

    Mono<Boolean> putAll(Map<? extends String, ? extends Map<String, Object>> map);

    Mono<Boolean> putIfAbsent(String key, Map<String, Object> value);

    Mono<Boolean> hasKey(String key);

    Flux<String> keys();

    Flux<Map<String, Object>> values();

    Flux<Map.Entry<String, Map<String, Object>>> entries();

    Mono<Long> size();

    Mono<Long> remove(String... keys);

    Mono<Boolean> delete();

}
