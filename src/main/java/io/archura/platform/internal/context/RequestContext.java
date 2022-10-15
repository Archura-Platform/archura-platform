package io.archura.platform.internal.context;

import io.archura.platform.api.cache.Cache;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.mapper.Mapper;
import io.archura.platform.api.publish.Publisher;
import io.archura.platform.api.stream.LightStream;
import lombok.Builder;
import lombok.Data;

import java.net.http.HttpClient;
import java.util.Optional;

@Data
@Builder(toBuilder = true)
public class RequestContext implements Context {

    private Optional<Cache> cache;
    private Optional<LightStream> lightStream;
    private Optional<Publisher> publisher;
    private Logger logger;
    private HttpClient httpClient;
    private Mapper mapper;

}
