package io.archura.platform.internal.configuration;

import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.lettuce.core.RedisURI;
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

public class CacheConfiguration {

    private final String redisUrl;
    private LettuceConnectionFactory redisConnectionFactory;

    public CacheConfiguration(final String redisUrl) {
        this.redisUrl = redisUrl;
    }

    public LettuceConnectionFactory getRedisConnectionFactory() {
        return redisConnectionFactory;
    }

    public void createRedisConnectionFactory() {
        final RedisURI redisURI = RedisURI.create(redisUrl);
        final RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(redisURI.getHost(), redisURI.getPort());
        redisConfiguration.setDatabase(redisURI.getDatabase());
        redisConfiguration.setUsername(redisURI.getUsername());
        if (nonNull(redisURI.getPassword())) {
            redisConfiguration.setPassword(RedisPassword.of(redisURI.getPassword()));
        }
        this.redisConnectionFactory = new LettuceConnectionFactory(redisConfiguration);
        this.redisConnectionFactory.afterPropertiesSet();
    }

    public HashOperations<String, String, Map<String, Object>> createHashOperations() {
        return redisTemplate().opsForHash();
    }

    private RedisTemplate<String, Map<String, Object>> redisTemplate() {

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

    public StreamOperations<String, Object, Object> createStreamOperations() {
        final StringRedisTemplate stringRedisTemplate = getStringRedisTemplate();
        return stringRedisTemplate.opsForStream();
    }

    private StringRedisTemplate getStringRedisTemplate() {
        return new StringRedisTemplate(redisConnectionFactory);
    }

}

