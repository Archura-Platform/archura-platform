package io.archura.platform.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.cache.Cache;
import io.archura.platform.logging.Logger;
import lombok.Builder;
import lombok.Data;

import java.net.http.HttpClient;
import java.util.Optional;

@Data
@Builder(toBuilder = true)
public class RequestContext implements Context {

    private Optional<Cache> cache;
    private Logger logger;
    private HttpClient httpClient;
    private ObjectMapper objectMapper;

}
