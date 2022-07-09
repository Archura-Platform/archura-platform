package io.archura.platform.configuration;


import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class FunctionsConfiguration {

    private Map<String, FunctionConfiguration> routeFunctions = new HashMap<>();

}
