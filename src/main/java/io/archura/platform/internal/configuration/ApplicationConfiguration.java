package io.archura.platform.internal.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.internal.Assets;
import io.archura.platform.internal.Initializer;
import io.archura.platform.internal.RequestHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.function.RequestPredicates;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Configuration
public class ApplicationConfiguration {

    @Value("${config.repository.url:http://config-service/}")
    private String configRepositoryUrl;
    private final HttpClient defaultHttpClient = buildDefaultHttpClient();
    private final HttpClient configurationHttpClient = buildConfigurationHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public ApplicationRunner prepareConfigurations(final Initializer initializer) {
        return args -> initializer.initialize();
    }

    @Bean("VirtualExecutorService")
    public ExecutorService getExecutorService() {
        final ThreadFactory factory = Thread.ofVirtual().name("VIRTUAL-THREAD").factory();
        return Executors.newCachedThreadPool(factory);
    }

    @Bean
    public TomcatProtocolHandlerCustomizer<?> tomcatProtocolHandlerCustomizer(
            @Qualifier("VirtualExecutorService") final ExecutorService executorService
    ) {
        return protocolHandler -> protocolHandler.setExecutor(executorService);
    }

    @Bean
    public TomcatServletWebServerFactory tomcatContainerFactory(
            @Qualifier("VirtualExecutorService") final ExecutorService executorService
    ) {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.setTomcatProtocolHandlerCustomizers(
                Collections.singletonList(tomcatProtocolHandlerCustomizer(executorService))
        );
        return factory;
    }


    @Bean
    public Assets assets() {
        return new Assets(objectMapper, defaultHttpClient);
    }

    @Bean
    public Initializer initializer(
            final ConfigurableBeanFactory beanFactory,
            @Qualifier("VirtualExecutorService") final ExecutorService executorService,
            final Assets assets
    ) {
        return new Initializer(configRepositoryUrl, configurationHttpClient, beanFactory, executorService, assets);
    }

    @Bean
    public RequestHandler requestHandler(
            final Assets assets,
            final ConfigurableBeanFactory beanFactory,
             @Qualifier("VirtualExecutorService") final ExecutorService executorService
   ) {
        return new RequestHandler(configRepositoryUrl, defaultHttpClient, assets, beanFactory, executorService);
    }

    @Bean
    public RouterFunction<ServerResponse> routes(final RequestHandler requestHandler) {
        return RouterFunctions.route()
                .route(RequestPredicates.GET("/producer"), requestHandler::handleProducer)
                .route(RequestPredicates.GET("/consumer"), requestHandler::handleConsumer)
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
