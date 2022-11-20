package io.archura.platform.internal.logging;


import java.io.PrintStream;

/**
 * Stream logger writes the log messages to the stream.
 */
public class StreamLogWriter implements LogWriter {

    /**
     * The default print stream points to the system output.
     * We can not use a logger since none of the logging solutions implement
     * a proper solution for Reactor and/or Webflux logging.
     */
    private static final PrintStream stream = System.out;

    /**
     * Writes the message on a new line using stream.
     *
     * @param message log message.
     */
    @Override
    public void log(String logLevel, String message) {
        stream.println(message);
    }

}