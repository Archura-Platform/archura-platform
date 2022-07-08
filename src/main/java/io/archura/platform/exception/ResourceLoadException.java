package io.archura.platform.exception;

public class ResourceLoadException extends RuntimeException {
    public ResourceLoadException(final Exception exception) {
        super(exception);
    }
}
