package io.archura.platform;//package io.archura.platform;
//
//import com.fasterxml.jackson.databind.type.MapType;
//import com.fasterxml.jackson.databind.type.TypeFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
//import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
//import org.springframework.data.redis.core.ReactiveHashOperations;
//import org.springframework.data.redis.core.ReactiveRedisTemplate;
//import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
//import org.springframework.data.redis.serializer.RedisSerializationContext;
//import org.springframework.data.redis.serializer.StringRedisSerializer;
//
//import java.util.Map;
//
//@Configuration
//public class CacheConfiguration {
//
//    @Bean
//    @Primary
//    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
//        return new LettuceConnectionFactory("centos", 6379);
//    }
//
//    @Bean
//    @Primary
//    public ReactiveHashOperations<String, String, Map<String, Object>> reactiveHashOperations(ReactiveRedisConnectionFactory factory) {
//        final MapType mapType = TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class);
//        RedisSerializationContext<String, Map<String, Object>> context = RedisSerializationContext
//                .<String, Map<String, Object>>newSerializationContext(new StringRedisSerializer())
//                .key(new StringRedisSerializer())
//                .value(new Jackson2JsonRedisSerializer<>(mapType))
//                .hashKey(new StringRedisSerializer())
//                .hashValue(new Jackson2JsonRedisSerializer<>(Map.class))
//                .build();
//
//        return new ReactiveRedisTemplate<>(factory, context).opsForHash();
//    }
//
//}
//
