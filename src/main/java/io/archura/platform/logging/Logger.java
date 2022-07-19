package io.archura.platform.logging;

import java.io.PrintStream;

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

    static Logger consoleLogger() {
        return new Logger() {

            private PrintStream out = System.out;

            @Override
            public void info(String message, Object... arguments) {
                log(message, arguments);
            }

            @Override
            public void debug(String message, Object... arguments) {
                log(message, arguments);
            }

            @Override
            public void error(String message, Object... arguments) {
                log(message, arguments);
            }

            private void log(String message, Object... arguments) {
                out.printf(String.format(message, arguments));
            }
        };
    }

}
