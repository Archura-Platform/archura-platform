package io.archura.platform.stream;


import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

public class RedisStreamSubscription {

    public Subscription createConsumerSubscription(
            final RedisConnectionFactory redisConnectionFactory,
            final StreamListener<String, ObjectRecord<String, byte[]>> streamListener,
            final String streamKey
    ) {
        final StreamMessageListenerContainer<String, ObjectRecord<String, byte[]>> listenerContainer = streamMessageListenerContainer(redisConnectionFactory);
        final Consumer consumer = Consumer.from(streamKey, getHostName());
        final StreamOffset<String> streamOffset = StreamOffset.create(streamKey, ReadOffset.lastConsumed());
        final Subscription subscription = listenerContainer.receive(consumer, streamOffset, streamListener);
        listenerContainer.start();
        return subscription;
    }

    private StreamMessageListenerContainer<String, ObjectRecord<String, byte[]>> streamMessageListenerContainer(
            final RedisConnectionFactory redisConnectionFactory
    ) {
        final StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, byte[]>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .targetType(byte[].class)
                .build();
        return StreamMessageListenerContainer.create(redisConnectionFactory, options);
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }

}