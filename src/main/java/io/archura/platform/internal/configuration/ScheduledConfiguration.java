package io.archura.platform.internal.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import io.archura.platform.api.attribute.GlobalKeys;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ScheduledConfiguration {
    private Map<String, EnvironmentConfiguration> environments = new HashMap<>();
    private Configuration config = new Configuration();

    @Data
    public static class EnvironmentConfiguration {
        private Map<String, TenantConfiguration> tenants = new HashMap<>();
        private Configuration config = new Configuration();
    }

    @Data
    public static class TenantConfiguration {
        private List<FunctionConfiguration> functions = new ArrayList<>();
        private Configuration config = new Configuration();
    }

    @Data
    public static class FunctionConfiguration {
        private String name;
        private String version;
        private long delay;
        private String timeUnit;
        private String logLevel;
        private JsonNode config;
    }

    @Data
    public static class ProducerConfiguration {
        private String name;
        private String version;
        private String topic;
        private Configuration config = new Configuration();
    }

    @Data
    public static class Configuration {
        private String logLevel = GlobalKeys.DEFAULT_LOG_LEVEL.getKey();
    }
}
