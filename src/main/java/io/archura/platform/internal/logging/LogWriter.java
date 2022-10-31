package io.archura.platform.internal.logging;


/**
 * A log writer interface that simply writes the already prepared log messages.
 */
public interface LogWriter {

    /**
     * Writes the log message using underlying stream.
     *
     * @param message log message.
     */
    void log(String logLevel, String message);

}
