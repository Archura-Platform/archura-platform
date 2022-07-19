package io.archura.platform.logging;


import java.io.PrintStream;

/**
 * A log writer interface that simply writes the already prepared log messages.
 */
public interface LogWriter {
    /**
     * The default print stream points to the system output.
     * We can not use a logger since none of the logging solutions implement
     * a proper solution for Reactor and/or Webflux logging.
     */
    PrintStream stream = System.out;

    /**
     * Creates a log writer.
     *
     * @return a LogWriter implementation.
     */
    static LogWriter create() {
        return new StreamLogWriter(stream);
    }

    /**
     * Writes the log message using underlying stream.
     *
     * @param logLevel log level of OFF, ERROR, WARNING, INFO, DEBUG, TRACE, ALL.
     * @param message  log message.
     */
    void log(String logLevel, String message);

}
