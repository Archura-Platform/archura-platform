package io.archura.platform.internal.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.Initializer;
import io.archura.platform.internal.RequestFilter;
import io.archura.platform.internal.RequestHandler;
import lombok.RequiredArgsConstructor;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@RequiredArgsConstructor
public class ApplicationConfiguration {

    private final String configRepositoryUrl;
    private final HttpClient defaultHttpClient = buildDefaultHttpClient();
    private final HttpClient configurationHttpClient = buildConfigurationHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ThreadFactory threadFactory = getThreadFactory();

    public RequestFilter requestInterceptor() {
        return new RequestFilter();
    }

    public ApplicationRunner prepareConfigurations(final Initializer initializer) {
        return args -> initializer.initialize();
    }

    public ExecutorService getExecutorService() {
        return Executors.newCachedThreadPool(threadFactory);
    }

    private ThreadFactory getThreadFactory() {
        return Thread.ofVirtual().name("VIRTUAL-THREAD").factory();
    }

    public Assets assets(
            final FilterFunctionExecutor filterFunctionExecutor
    ) {
        return new Assets(objectMapper, defaultHttpClient, filterFunctionExecutor);
    }

    public Initializer initializer(
            final FilterFunctionExecutor filterFunctionExecutor,
            final ExecutorService executorService,
            final Assets assets
    ) {
        return new Initializer(
                configRepositoryUrl,
                configurationHttpClient,
                executorService,
                assets,
                filterFunctionExecutor
        );
    }

    public RequestHandler requestHandler(
            final Assets assets,
            final ConfigurableBeanFactory beanFactory,
            final FilterFunctionExecutor filterFunctionExecutor,
            @Qualifier("VirtualExecutorService") final ExecutorService executorService
    ) {
        return new RequestHandler(configRepositoryUrl, defaultHttpClient, assets, beanFactory, filterFunctionExecutor);
    }

    public RouterFunction<ServerResponse> routes(final RequestHandler requestHandler) {
        return RouterFunctions.route()
                .route(RequestPredicates.all(), requestHandler::handle)
                .build();
    }

    private HttpClient buildDefaultHttpClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    private HttpClient buildConfigurationHttpClient() {
        return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

}
