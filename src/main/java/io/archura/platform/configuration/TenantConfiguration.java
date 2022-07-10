package io.archura.platform.configuration;


import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class TenantConfiguration {

    private List<PreFilterConfiguration> pre = new ArrayList<>();
    private List<PostFilterConfiguration> post = new ArrayList<>();
    private Map<String, FunctionConfiguration> routeFunctions = new HashMap<>();

}
