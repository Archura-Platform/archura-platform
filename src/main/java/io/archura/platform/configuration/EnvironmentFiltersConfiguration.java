package io.archura.platform.configuration;


import lombok.Data;

import java.util.List;

@Data
public class EnvironmentFiltersConfiguration {

    private List<PreFilter> pre;
    private List<PostFilter> post;

}
