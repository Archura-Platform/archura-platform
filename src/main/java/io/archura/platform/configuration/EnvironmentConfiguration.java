package io.archura.platform.configuration;


import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class EnvironmentConfiguration implements FilterConfiguration {
    private List<PreFilter> pre = new ArrayList<>();
    private List<PostFilter> post = new ArrayList<>();
    private Map<String, TenantConfiguration> tenantConfigurations = new HashMap<>();
}
