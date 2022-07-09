package io.archura.platform.exception;

public class JsonReadException extends RuntimeException {
    public JsonReadException(final Exception exception) {
        super(exception);
    }
}
