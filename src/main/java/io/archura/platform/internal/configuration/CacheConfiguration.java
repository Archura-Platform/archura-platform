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
    private HashOperations<String, String, Map<String, Object>> hashOperations;
    private StreamOperations<String, Object, Object> streamOperations;

    public CacheConfiguration(final String redisUrl) {
        this.redisUrl = redisUrl;
    }

    public LettuceConnectionFactory getRedisConnectionFactory() {
        return redisConnectionFactory;
    }

    public HashOperations<String, String, Map<String, Object>> getHashOperations() {
        return hashOperations;
    }

    public StreamOperations<String, Object, Object> getStreamOperations() {
        return streamOperations;
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

    public void createHashOperations() {
        this.hashOperations = getRedisTemplate().opsForHash();
    }

    public void createStreamOperations() {
        final StringRedisTemplate stringRedisTemplate = getStringRedisTemplate();
        stringRedisTemplate.afterPropertiesSet();
        this.streamOperations = stringRedisTemplate.opsForStream();
    }

    private RedisTemplate<String, Map<String, Object>> getRedisTemplate() {
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

    private StringRedisTemplate getStringRedisTemplate() {
        return new StringRedisTemplate(redisConnectionFactory);
    }

}

