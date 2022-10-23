package io.archura.platform.internal.exception;

public class ResourceLoadException extends RuntimeException {
    public ResourceLoadException(final Exception exception) {
        super(exception);
    }
}
