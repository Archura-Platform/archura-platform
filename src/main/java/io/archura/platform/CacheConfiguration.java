package io.archura.platform;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;

@Configuration
public class CacheConfiguration {

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        return new LettuceConnectionFactory();
    }

    @Bean
    public RedisTemplate<String, Map<String, Object>> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Map<String, Object>> tempTemplate = new RedisTemplate<>();
        tempTemplate.setConnectionFactory(redisConnectionFactory);
        return tempTemplate;
    }

    @Bean
    public HashOperations<String, String, Map<String, Object>> redisTemplate(RedisTemplate<String, Map<String, Object>> redisTemplate) {
        return redisTemplate.opsForHash();
    }

}

