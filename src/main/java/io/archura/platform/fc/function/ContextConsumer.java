package io.archura.platform.fc.function;

import io.archura.platform.context.Context;

import java.util.function.Consumer;

@FunctionalInterface
public interface ContextConsumer extends Consumer<Context> {
}
