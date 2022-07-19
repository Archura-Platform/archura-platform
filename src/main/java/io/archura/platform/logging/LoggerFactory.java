package io.archura.platform.logging;

import java.util.Map;

/**
 * A factory to create loggers.
 */
public interface LoggerFactory {

    /**
     * Creates a Logger and sets the attributes.
     *
     * @param attributes key value pairs.
     * @return a Logger implementation.
     */
    static Logger create(final Map<String, Object> attributes) {
        return DefaultLogger.create(attributes);
    }

}
