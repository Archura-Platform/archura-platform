package io.archura.platform.function;

@FunctionalInterface
public interface StreamConsumer {

    void consume(byte[] key, byte[] value);

}
