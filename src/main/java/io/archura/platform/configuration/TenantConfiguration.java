package io.archura.platform.configuration;


import lombok.Data;

import java.util.List;

@Data
public class TenantConfiguration implements FilterConfiguration {

    private List<PreFilter> pre;
    private List<PostFilter> post;

}
