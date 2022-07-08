package io.archura.platform.configuration;

import lombok.Data;

@Data
public class Configuration {

    private GlobalConfiguration globalConfiguration;
    private EnvironmentConfiguration environmentConfiguration;
    private TenantConfiguration tenantConfiguration;
    private FunctionsConfiguration functionsConfiguration;

}
