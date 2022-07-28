package io.archura.platform.internal.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import io.archura.platform.api.attribute.GlobalKeys;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class GlobalConfiguration {
    private List<PreFilterConfiguration> pre = new ArrayList<>();
    private List<PostFilterConfiguration> post = new ArrayList<>();
    private Map<String, EnvironmentConfiguration> environments = new HashMap<>();
    private GlobalConfig config = new GlobalConfig();
    private CacheConfiguration cacheConfiguration;

    @Data
    public static class GlobalConfig extends Configuration {
        private String redisUrl;
        private String codeRepositoryUrl;
        private String archuraPlatformToken;
    }

    @Data
    public static class PreFilterConfiguration {
        private String name;
        private String version;
        private JsonNode config;
    }

    @Data
    public static class PostFilterConfiguration {
        private String name;
        private String version;
        private JsonNode config;
    }

    @Data
    public static class EnvironmentConfiguration {
        private List<PreFilterConfiguration> pre = new ArrayList<>();
        private List<PostFilterConfiguration> post = new ArrayList<>();
        private Map<String, TenantConfiguration> tenants = new HashMap<>();
    }


    @Data
    public static class TenantConfiguration {

        private List<PreFilterConfiguration> pre = new ArrayList<>();
        private List<PostFilterConfiguration> post = new ArrayList<>();
        private Map<String, TenantConfiguration.RouteConfiguration> routes = new HashMap<>();

        @Data
        public static class RouteConfiguration {
            private List<PreFilterConfiguration> pre = new ArrayList<>();
            private List<PostFilterConfiguration> post = new ArrayList<>();
            private TenantConfiguration.RouteConfiguration.FunctionConfiguration function;

            @Data
            public static class FunctionConfiguration {
                private String name;
                private String version;
                private JsonNode config;
            }
        }

    }

    @Data
    public static class Configuration {
        private String logLevel = GlobalKeys.DEFAULT_LOG_LEVEL.getKey();
    }
}
