package io.archura.platform.configuration;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Configuration {

    private GlobalConfiguration globalConfiguration;
    private Map<String, EnvironmentConfiguration> environmentConfigurations = new HashMap<>();
    private FunctionsConfiguration functionsConfiguration = new FunctionsConfiguration();

}
