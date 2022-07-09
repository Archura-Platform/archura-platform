package io.archura.platform.configuration;

import java.util.List;

public interface FilterConfiguration {
    List<PreFilter> getPre();

    List<PostFilter> getPost();
}
