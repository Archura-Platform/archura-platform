package io.archura.platform.configuration;


import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class FunctionsConfiguration {

    private Map<String, Function> routeFunctions;

}
