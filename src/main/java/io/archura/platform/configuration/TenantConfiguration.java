package io.archura.platform.configuration;


import lombok.Data;

import java.util.List;

@Data
public class TenantConfiguration {

    private List<PreFilter> preFilters;
    private List<PostFilter> postFilters;

}
