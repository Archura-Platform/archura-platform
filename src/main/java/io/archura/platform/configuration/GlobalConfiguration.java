package io.archura.platform.configuration;


import lombok.Data;

import java.util.List;

@Data
public class GlobalConfiguration {

    private List<PreFilter> preFilters;
    private List<PostFilter> postFilters;

}
