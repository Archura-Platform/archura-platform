package io.archura.platform.internal.configuration;

import io.archura.platform.api.mapper.Mapper;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.Initializer;
import io.archura.platform.internal.RequestHandler;
import io.archura.platform.internal.mapper.ObjectMapper;
import io.archura.platform.internal.server.HttpServer;
import io.archura.platform.internal.server.Server;
import lombok.RequiredArgsConstructor;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@RequiredArgsConstructor
public class ApplicationConfiguration {

    public ThreadFactory threadFactory() {
        return Thread.ofVirtual().name("VIRTUAL-THREAD").factory();
    }

    public ExecutorService executorService(ThreadFactory threadFactory) {
        return Executors.newCachedThreadPool(threadFactory);
    }

    public FilterFunctionExecutor filterFunctionExecutor() {
        return new FilterFunctionExecutor();
    }

    public Mapper objectMapper() {
        return new ObjectMapper(new com.fasterxml.jackson.databind.ObjectMapper());
    }

    public HttpClient httpClient() {
        return HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public Assets assets(
            final Mapper mapper,
            final HttpClient httpClient,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        return new Assets(mapper, httpClient, filterFunctionExecutor);
    }

    public Initializer initializer(
            final String configRepositoryUrl,
            final HttpClient httpClient,
            final ThreadFactory threadFactory,
            final ExecutorService executorService,
            final Assets assets,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        return new Initializer(
                configRepositoryUrl,
                httpClient,
                threadFactory,
                executorService,
                assets,
                filterFunctionExecutor
        );
    }

    public RequestHandler requestHandler(
            final String configRepositoryUrl,
            final HttpClient httpClient,
            final Assets assets,
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        return new RequestHandler(configRepositoryUrl, httpClient, assets, filterFunctionExecutor);
    }

    public Server server() {
        return new HttpServer();
    }

}
