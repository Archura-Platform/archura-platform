package io.archura.platform.internal.cache;

import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.lettuce.core.RedisURI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Map;

import static java.util.Objects.nonNull;

@Configuration
@ConditionalOnProperty(name = "spring.redis.url")
public class CacheConfiguration {

    @Value("${spring.redis.url}")
    private String redisUrl;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        final RedisURI redisURI = RedisURI.create(redisUrl);
        final RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(redisURI.getHost(), redisURI.getPort());
        redisConfiguration.setDatabase(redisURI.getDatabase());
        redisConfiguration.setUsername(redisURI.getUsername());
        if (nonNull(redisURI.getPassword())) {
            redisConfiguration.setPassword(RedisPassword.of(redisURI.getPassword()));
        }
        return new LettuceConnectionFactory(redisConfiguration);
    }

    @Bean
    public RedisTemplate<String, Map<String, Object>> redisTemplate(RedisConnectionFactory redisConnectionFactory) {

        final MapType mapType = TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class);
        final RedisTemplate<String, Map<String, Object>> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setHashValueSerializer(new Jackson2JsonRedisSerializer<>(Map.class));
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new Jackson2JsonRedisSerializer<>(mapType));
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());

        redisTemplate.afterPropertiesSet();

        return redisTemplate;
    }

    @Bean
    public HashOperations<String, String, Map<String, Object>> hashOperations(RedisTemplate<String, Map<String, Object>> redisTemplate) {
        return redisTemplate.opsForHash();
    }

    @Bean
    public StreamOperations<String, Object, Object> streamOperations(StringRedisTemplate stringRedisTemplate) {
        return stringRedisTemplate.opsForStream();
    }

}

