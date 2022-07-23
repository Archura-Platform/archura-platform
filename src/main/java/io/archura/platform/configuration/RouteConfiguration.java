package io.archura.platform.configuration;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class RouteConfiguration {
    private List<PreFilterConfiguration> pre = new ArrayList<>();
    private List<PostFilterConfiguration> post = new ArrayList<>();
    private FunctionConfiguration function;
}
