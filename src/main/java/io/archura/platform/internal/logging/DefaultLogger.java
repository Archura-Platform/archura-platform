package io.archura.platform.internal.logging;

import io.archura.platform.api.attribute.*;
import io.archura.platform.api.logger.Logger;
import jdk.internal.reflect.Reflection;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;


/**
 * Prepares the necessary data for the log message.
 */
public class DefaultLogger implements Logger {

    static {
        Reflection.registerFieldsToFilter(DefaultLogger.class, Set.of("environment", "tenantId"));
    }

    private static final Map<String, String> colors = Map.of(
            "_RESET_", "\u001B[0m",
            "_RED_", "\u001B[31m",
            "_GREEN_", "\u001B[32m",
            "_YELLOW_", "\u001B[33m",
            "_BLUE_", "\u001B[34m",
            "_PURPLE_", "\u001B[35m",
            "_CYAN_", "\u001B[36m",
            "_WHITE_", "\u001B[37m"
    );
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    private static final LogWriter logWriter = LogWriter.create();
    private static final String DEFAULT_LOG_TEMPLATE = "%s _GREEN_%5s_RESET_ [_BLUE_%s_RESET_][_PURPLE_%s_RESET_][%18s]_CYAN_[%50s]_RESET_ - %s";

    private final System.Logger.Level currentLogLevel;
    private final String environment;
    private final Object tenantId;
    private final String logTemplate;

    private DefaultLogger(System.Logger.Level currentLogLevel, String environment, Object tenantId, String logTemplate) {
        this.currentLogLevel = currentLogLevel;
        this.environment = environment;
        this.tenantId = tenantId;
        this.logTemplate = logTemplate;
    }

    static Logger create(final Map<String, Object> attributes) {
        String logTemplate = String.valueOf(attributes.getOrDefault(GlobalKeys.REQUEST_LOG_TEMPLATE.getKey(), DEFAULT_LOG_TEMPLATE));
        for (Map.Entry<String, String> entry : colors.entrySet()) {
            logTemplate = logTemplate.replace(entry.getKey(), entry.getValue());
        }
        final String environment = String.valueOf(attributes.getOrDefault(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), GlobalKeys.ENVIRONMENT_NOT_SET.getKey()));
        final Object tenantId = attributes.getOrDefault(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.TENANT_NOT_SET.getKey());
        final String currentLogLevelValue = String.valueOf(attributes.getOrDefault(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), GlobalKeys.DEFAULT_LOG_LEVEL.getKey()));
        final System.Logger.Level currentLogLevel = Stream.of(System.Logger.Level.values()).filter(level -> level.getName().equalsIgnoreCase(currentLogLevelValue)).findFirst().orElse(System.Logger.Level.INFO);
        return new DefaultLogger(currentLogLevel, environment, tenantId, logTemplate);
    }

    /**
     * Logs the log message if the log level is INFO.
     *
     * @param message   log message.
     * @param arguments log arguments.
     */
    public void info(String message, Object... arguments) {
        if (System.Logger.Level.INFO.getSeverity() >= currentLogLevel.getSeverity()) {
            log(System.Logger.Level.INFO.getName(), message, arguments);
        }
    }

    /**
     * Logs the log message if the log level is DEBUG.
     *
     * @param message   log message.
     * @param arguments log arguments.
     */
    public void debug(String message, Object... arguments) {
        if (System.Logger.Level.DEBUG.getSeverity() >= currentLogLevel.getSeverity()) {
            log(System.Logger.Level.DEBUG.getName(), message, arguments);
        }
    }

    /**
     * Logs the log message if the log level is ERROR.
     *
     * @param message   log message.
     * @param arguments log arguments.
     */
    public void error(String message, Object... arguments) {
        if (System.Logger.Level.ERROR.getSeverity() >= currentLogLevel.getSeverity()) {
            log(System.Logger.Level.ERROR.getName(), message, arguments);
        }
    }

    private void log(String logLevel, String message, Object[] arguments) {
        final StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        final String classMethodLine = element.getClassName() + "." + element.getMethodName() + "():" + element.getLineNumber();
        final String classMethodLineSub = classMethodLine.length() > 50 ? classMethodLine.substring(classMethodLine.length() - 50) : classMethodLine;

        final String thread = String.format("%s-%s", Thread.currentThread().threadId(), Thread.currentThread().getName());
        final String logMessage = String.format(message, arguments);
        final String date = DATETIME_FORMATTER.format(LocalDateTime.now());
        logWriter.log(logLevel, String.format(logTemplate,
                date, logLevel, environment, tenantId, thread, classMethodLineSub, logMessage));
    }

}
