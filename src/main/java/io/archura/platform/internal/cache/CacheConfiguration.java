package io.archura.platform.internal.cache;

import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.beans.factory.annotation.Value;
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
public class CacheConfiguration {

    @Value("${redis.host:localhost}")
    private String redisHost;

    @Value("${redis.port:6379}")
    private int redisPort;

    @Value("${redis.database:0}")
    private int redisDatabase;

    @Value("${redis.user:#{null}}")
    private String redisUser;

    @Value("${redis.password:#{null}}")
    private String redisPassword;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        final RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(redisHost, redisPort);
        redisConfiguration.setDatabase(redisDatabase);
        if (nonNull(redisUser)) {
            redisConfiguration.setUsername(redisUser);
        }
        if (nonNull(redisPassword)) {
            redisConfiguration.setPassword(RedisPassword.of(redisPassword));
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

