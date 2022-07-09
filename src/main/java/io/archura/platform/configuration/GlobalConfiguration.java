package io.archura.platform.configuration;


import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class GlobalConfiguration implements FilterConfiguration {

    private List<PreFilter> pre = new ArrayList<>();
    private List<PostFilter> post = new ArrayList<>();

}
