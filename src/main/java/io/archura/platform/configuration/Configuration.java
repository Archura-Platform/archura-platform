package io.archura.platform.configuration;

import lombok.Data;

@Data
public class Configuration {

    private GlobalFiltersConfiguration globalFiltersConfiguration;
    private EnvironmentFiltersConfiguration environmentFiltersConfiguration;
    private TenantFiltersConfiguration tenantFiltersConfiguration;
    private FunctionsConfiguration functionsConfiguration;

}
