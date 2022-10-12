package io.archura.platform.internal.publish;

public interface MessagePublisher {
    void publish(String channel, String message);
}
