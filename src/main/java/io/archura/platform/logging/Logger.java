package io.archura.platform.logging;

public interface Logger {

    /**
     * Logs the message if the log level is INFO.
     *
     * @param message   log message.
     * @param arguments log arguments.
     */
    void info(String message, Object... arguments);

    /**
     * Logs the message if the log level is DEBUG.
     *
     * @param message   log message.
     * @param arguments log arguments.
     */
    void debug(String message, Object... arguments);

    /**
     * Logs the message if the log level is ERROR.
     *
     * @param message   log message.
     * @param arguments log arguments.
     */
    void error(String message, Object... arguments);

}
