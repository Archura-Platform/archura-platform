package io.archura.platform.internal.exception;

public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigurationException(final Exception exception) {
        super(exception);
    }

    public ConfigurationException(String message) {
        super(message);
    }
}
