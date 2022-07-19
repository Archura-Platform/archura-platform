package io.archura.platform.context;

import io.archura.platform.cache.Cache;
import io.archura.platform.logging.Logger;

import java.net.http.HttpClient;
import java.util.Optional;

public interface Context {

    Optional<Cache> getCache();

    Logger getLogger();

    HttpClient getHttpClient();

}
