package io.archura.platform.logging;


import java.io.PrintStream;

/**
 * Stream logger writes the log messages to the stream.
 */
public class StreamLogWriter implements LogWriter {

    private final PrintStream stream;

    public StreamLogWriter(PrintStream printStream) {
        this.stream = printStream;
    }

    /**
     * Writes the message on a new line using stream.
     *
     * @param logLevel log level of OFF, ERROR, WARNING, INFO, DEBUG, TRACE, ALL.
     * @param message  log message.
     */
    @Override
    public void log(String logLevel, String message) {
        stream.println(message);
    }

}