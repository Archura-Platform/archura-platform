package io.archura.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.Initializer;
import io.archura.platform.internal.RequestHandler;
import io.archura.platform.internal.configuration.ApplicationConfiguration;
import io.archura.platform.internal.server.Server;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

public class ArchuraPlatformApplication {

    public static void main(String[] args) throws Exception {
        final ArchuraPlatformApplication application = new ArchuraPlatformApplication();
        application.start();
    }

    private void start() throws IOException {
        final String configRepositoryUrl = Optional.ofNullable(System.getenv("CONFIG_REPOSITORY_URL")).orElse("http://config-service");
        final ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
        final ThreadFactory threadFactory = applicationConfiguration.threadFactory();
        final ExecutorService executorService = applicationConfiguration.executorService(threadFactory);
        final ObjectMapper objectMapper = applicationConfiguration.objectMapper();
        final HttpClient httpClient = applicationConfiguration.httpClient();
        final FilterFunctionExecutor filterFunctionExecutor = applicationConfiguration.filterFunctionExecutor();
        final Assets assets = applicationConfiguration.assets(objectMapper, httpClient, filterFunctionExecutor);
        final Initializer initializer = applicationConfiguration.initializer(configRepositoryUrl, httpClient, threadFactory, executorService, assets, filterFunctionExecutor);
        initializer.initialize();

        final RequestHandler requestHandler = applicationConfiguration.requestHandler(configRepositoryUrl, httpClient, assets, filterFunctionExecutor);
        final Server server = applicationConfiguration.server();
        server.start(requestHandler, executorService, assets);
    }

}
