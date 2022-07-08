package io.archura.platform.exception;

public class ConfigurationException extends RuntimeException {
    public ConfigurationException(final Exception exception) {
        super(exception);
    }
}
