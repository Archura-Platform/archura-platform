package io.archura.platform.internal.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import io.archura.platform.api.attribute.GlobalKeys;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SubscribedConfiguration {
    private Map<String, EnvironmentConfiguration> environments = new HashMap<>();
    private Configuration config = new Configuration();

    @Data
    public static class EnvironmentConfiguration {
        private Map<String, TenantConfiguration> tenants = new HashMap<>();
        private Configuration config = new Configuration();
    }

    @Data
    public static class TenantConfiguration {
        private List<ConsumerConfiguration> subscribers = new ArrayList<>();
        private Configuration config = new Configuration();
    }

    @Data
    public static class ConsumerConfiguration {
        private String name;
        private String version;
        private String channel;
        private String logLevel;
        private JsonNode config;
    }

    @Data
    public static class Configuration {
        private String logLevel = GlobalKeys.DEFAULT_LOG_LEVEL.getKey();
    }
}
