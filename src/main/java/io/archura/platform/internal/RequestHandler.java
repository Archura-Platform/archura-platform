package io.archura.platform.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.attribute.TenantKeys;
import io.archura.platform.api.context.Context;
import io.archura.platform.api.exception.ErrorDetail;
import io.archura.platform.api.exception.FunctionIsNotAHandlerFunctionException;
import io.archura.platform.api.exception.PostFilterIsNotABiFunctionException;
import io.archura.platform.api.exception.PreFilterIsNotAUnaryOperatorException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.functionalcore.StreamConsumer;
import io.archura.platform.api.type.functionalcore.StreamProducer;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.stream.Movie;
import io.archura.platform.internal.stream.RedisStreamSubscription;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class RequestHandler {

    private final String configRepositoryUrl;
    private final HttpClient defaultHttpClient;
    private final Assets assets;
    private final ConfigurableBeanFactory beanFactory;
    private final ExecutorService executorService;

    public ServerResponse handleProducer(ServerRequest request) {
        final GlobalConfiguration globalConfiguration = beanFactory.getBean(GlobalConfiguration.class);
        final String environment = "default";
        final String tenantId = "default";
        final String topicNameFromConfiguration = "key1";
        final String streamKey = String.format("%s-%s-%s", environment, tenantId, topicNameFromConfiguration);
        // CREATE CONTEXT
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), environment);
        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), tenantId);
        final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
        assets.rebuildContext(attributes, hashOperations);
        final Context context = (Context) attributes.get(Context.class.getSimpleName());
        final Logger logger = context.getLogger();
        final StreamProducer movieProducer = c -> {
            final ObjectMapper objectMapper = c.getObjectMapper();
            final Movie movie = new Movie("Movie Title", 1977);
            String movieString = "";
            try {
                movieString = objectMapper.writeValueAsString(movie);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            final String key = String.valueOf(System.currentTimeMillis());
            return new AbstractMap.SimpleEntry<>(
                    key.getBytes(StandardCharsets.UTF_8),
                    movieString.getBytes(StandardCharsets.UTF_8));
        };
        Map.Entry<byte[], byte[]> entry = movieProducer.produce(context);

        ObjectRecord<String, byte[]> streamRecord = StreamRecords.newRecord()
                .ofObject(entry.getValue())
                .withStreamKey(streamKey);
        final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();
        final RecordId add = streamOperations.add(streamRecord);
        logger.info("streamOperations add = " + add);
        return ServerResponse.ok().build();
    }

    public ServerResponse handleConsumer(ServerRequest serverRequest) {
        final GlobalConfiguration globalConfiguration = beanFactory.getBean(GlobalConfiguration.class);
        // CREATE FUNCTION FROM CONFIGURATION
        final StreamConsumer movieConsumer = (context, key, value) -> {
            try {
                final Logger logger = context.getLogger();
                final ObjectMapper objectMapper = context.getObjectMapper();
                final Movie movie = objectMapper.readValue(value, Movie.class);
                logger.info("key: %s, value: %s", new String(key), movie);
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        // TRY AND REGISTER SINGLETON BEAN FOR FUNCTION
        final String environment = "default";
        final String tenantId = "default";
        final String topicNameFromConfiguration = "key1";
        final String topicName = String.format("%s-%s-%s", environment, tenantId, topicNameFromConfiguration);
        final String topicConsumerBeanName = String.format("%s-%s", topicName, movieConsumer.hashCode());
        // CREATE CONTEXT
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(GlobalKeys.REQUEST_ENVIRONMENT.getKey(), environment);
        attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), tenantId);
        final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
        assets.rebuildContext(attributes, hashOperations);
        final Context context = (Context) attributes.get(Context.class.getSimpleName());
        final Logger logger = context.getLogger();
        // CREATE BEAN
        try {
            beanFactory.isSingleton(topicConsumerBeanName);
        } catch (NoSuchBeanDefinitionException e) {
            //
            // IF STREAM TYPE IS REDIS
            //
            // CREATE STREAM AND GROUP FOR ENV-TENANT-TOPIC
            final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();
            final StreamInfo.XInfoGroups groups = streamOperations.groups(topicName);
            if (groups.isEmpty()) {
                final String group = streamOperations.createGroup(topicName, topicName);
                logger.info("group = " + group);
            }
            // CREATE REDIS BEAN
            final StreamListener<String, ObjectRecord<String, byte[]>> redisStreamListener =
                    message -> movieConsumer.consume(context, message.getId().getValue().getBytes(StandardCharsets.UTF_8), message.getValue());
            final LettuceConnectionFactory redisConnectionFactory = globalConfiguration.getCacheConfiguration().getRedisConnectionFactory();
            final RedisStreamSubscription redisStreamSubscription = new RedisStreamSubscription();
            final Subscription redisStreamFunctionSubscription = redisStreamSubscription.createConsumerSubscription(
                    redisConnectionFactory,
                    redisStreamListener,
                    topicName,
                    executorService
            );
            // REGISTER REDIS BEAN
            beanFactory.registerSingleton(topicConsumerBeanName, redisStreamFunctionSubscription);
            //
            //
            //
            //
            //
            // IF STREAM TYPE IS KAFKA
            //
        }
        final Object topicConsumerBean = beanFactory.getBean(topicConsumerBeanName);
        logger.info("topicConsumerBeanName = " + topicConsumerBeanName + " topicConsumerBean = " + topicConsumerBean);
        return ServerResponse.ok().build();
    }

    public ServerResponse handle(ServerRequest request) {
        try {
            final GlobalConfiguration globalConfiguration = beanFactory.getBean(GlobalConfiguration.class);
            final String logLevel = globalConfiguration.getConfig().getLogLevel();
            final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();

            final Map<String, Object> attributes = request.attributes();
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
            assets.rebuildContext(attributes, hashOperations);

            final List<UnaryOperator<ServerRequest>> globalPreFilters = getGlobalPreFilters(
                    globalConfiguration.getPre(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (UnaryOperator<ServerRequest> preFilter : globalPreFilters) {
                assets.getLogger(attributes).debug("Will run global PreFilter: %s", preFilter.getClass().getSimpleName());
                request = preFilter.apply(request);
                assets.rebuildContext(attributes, hashOperations);
            }

            String environmentName = String.valueOf(attributes.get(GlobalKeys.REQUEST_ENVIRONMENT.getKey()));
            final List<UnaryOperator<ServerRequest>> environmentPreFilters = getEnvironmentPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (UnaryOperator<ServerRequest> preFilter : environmentPreFilters) {
                assets.getLogger(attributes).debug("Will run environment PreFilter: %s", preFilter.getClass().getSimpleName());
                request = preFilter.apply(request);
                assets.rebuildContext(attributes, hashOperations);
            }
            /* REMOVE */
            attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.DEFAULT_TENANT_ID.getKey());
            assets.rebuildContext(attributes, hashOperations);

            String tenantId = String.valueOf(attributes.get(EnvironmentKeys.REQUEST_TENANT_ID.getKey()));
            final List<UnaryOperator<ServerRequest>> tenantPreFilters = getTenantPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (UnaryOperator<ServerRequest> preFilter : tenantPreFilters) {
                assets.getLogger(attributes).debug("Will run tenant PreFilter: %s", preFilter.getClass().getSimpleName());
                request = preFilter.apply(request);
                assets.rebuildContext(attributes, hashOperations);
            }

            final String routeId = request.attribute(TenantKeys.ROUTE_ID.getKey()).map(String::valueOf).orElse(TenantKeys.CATCH_ALL_ROUTE_KEY.getKey());
            final List<UnaryOperator<ServerRequest>> routePreFilters = getRoutePreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (UnaryOperator<ServerRequest> preFilter : routePreFilters) {
                assets.getLogger(attributes).debug("Will run route PreFilter: %s", preFilter.getClass().getSimpleName());
                request = preFilter.apply(request);
                assets.rebuildContext(attributes, hashOperations);
            }

            final Optional<HandlerFunction<ServerResponse>> tenantFunction = getTenantFunctions(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            assets.getLogger(attributes).debug("Will run TenantFunction: %s", tenantFunction);
            ServerResponse response = tenantFunction
                    .orElse(r -> ServerResponse
                            .notFound()
                            .header(String.format("X-A-NotFound-%s-%s-%s", environmentName, tenantId, routeId))
                            .build()
                    )
                    .handle(request);

            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> routePostFilters = getRoutePostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : routePostFilters) {
                assets.getLogger(attributes).debug("Will run route PostFilter: %s", postFilter.getClass().getSimpleName());
                response = postFilter.apply(request, response);
            }

            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> tenantPostFilters = getTenantPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : tenantPostFilters) {
                assets.getLogger(attributes).debug("Will run tenant PostFilter: %s", postFilter.getClass().getSimpleName());
                response = postFilter.apply(request, response);
            }

            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> environmentPostFilters = getEnvironmentPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : environmentPostFilters) {
                assets.getLogger(attributes).debug("Will run environment PostFilter: %s", postFilter.getClass().getSimpleName());
                response = postFilter.apply(request, response);
            }
            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> globalPostFilters = getGlobalPostFilters(
                    globalConfiguration.getPost(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : globalPostFilters) {
                assets.getLogger(attributes).debug("Will run global PostFilter: %s", postFilter.getClass().getSimpleName());
                response = postFilter.apply(request, response);
            }
            return response;
        } catch (Exception e) {
            return this.getErrorResponse(e, request);
        }
    }


    private List<UnaryOperator<ServerRequest>> getGlobalPreFilters(
            final List<GlobalConfiguration.PreFilterConfiguration> preFilterConfigurations,
            final String codeRepositoryUrl
    ) {
        return preFilterConfigurations
                .stream()
                .map(preFilterConfig -> getPreFilter(codeRepositoryUrl, preFilterConfig, "global"))
                .toList();
    }

    private List<UnaryOperator<ServerRequest>> getEnvironmentPreFilters(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName
    ) {
        final GlobalConfiguration globalConfiguration;

        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            String environmentConfigURL = String.format("%s/imperative-shell/environments/%s/config.json", configRepositoryUrl, environmentName);
            GlobalConfiguration.EnvironmentConfiguration environmentConfig = getEnvironmentConfiguration(environmentConfigURL);
            environments.put(environmentName, environmentConfig);
        }
        return environments.get(environmentName)
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s", environmentName)))
                .toList();
    }

    private GlobalConfiguration.EnvironmentConfiguration getEnvironmentConfiguration(String url) {
        return assets.getConfiguration(defaultHttpClient, url, GlobalConfiguration.EnvironmentConfiguration.class);
    }

    private List<UnaryOperator<ServerRequest>> getTenantPreFilters(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName,
            final String tenantId
    ) {
        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        final GlobalConfiguration.TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            String tenantConfigURL = String.format("%s/imperative-shell/environments/%s/tenants/%s/config.json", configRepositoryUrl, environmentName, tenantId);
            GlobalConfiguration.TenantConfiguration tenantConfig = getTenantConfiguration(tenantConfigURL);
            environmentConfiguration.getTenants().put(tenantId, tenantConfig);
        }
        return environmentConfiguration.getTenants().get(tenantId)
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)))
                .toList();
    }

    private List<UnaryOperator<ServerRequest>> getRoutePreFilters(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName,
            final String tenantId,
            final String routeId
    ) {
        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        final GlobalConfiguration.TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            return Collections.emptyList();
        }
        final GlobalConfiguration.TenantConfiguration.RouteConfiguration routeConfiguration = tenantConfiguration.getRoutes().get(routeId);
        if (isNull(routeConfiguration)) {
            return Collections.emptyList();
        }
        return routeConfiguration
                .getPre()
                .stream()
                .map(preFilter -> getPreFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)))
                .toList();
    }

    private GlobalConfiguration.TenantConfiguration getTenantConfiguration(String url) {
        return assets.getConfiguration(defaultHttpClient, url, GlobalConfiguration.TenantConfiguration.class);
    }

    private UnaryOperator<ServerRequest> getPreFilter(String codeServerURL, GlobalConfiguration.PreFilterConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (UnaryOperator.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final UnaryOperator<ServerRequest> consumer = (UnaryOperator<ServerRequest>) object;
                return consumer;
            } else {
                throw new PreFilterIsNotAUnaryOperatorException(String.format("Resource is not a UnaryOperator, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private Optional<HandlerFunction<ServerResponse>> getTenantFunctions(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName,
            final String tenantId,
            final String routeId
    ) {
        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Optional.empty();
        }
        final GlobalConfiguration.TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            return Optional.empty();
        }
        GlobalConfiguration.TenantConfiguration.RouteConfiguration routeConfiguration = tenantConfiguration.getRoutes().get(routeId);
        if (nonNull(routeConfiguration)) {
            GlobalConfiguration.TenantConfiguration.RouteConfiguration.FunctionConfiguration functionConfiguration = routeConfiguration.getFunction();
            if (nonNull(functionConfiguration)) {
                return Optional.of(getFunction(codeRepositoryUrl, functionConfiguration, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)));
            }
        }
        GlobalConfiguration.TenantConfiguration.RouteConfiguration routeConfigurationCatchAll = tenantConfiguration.getRoutes().get(TenantKeys.CATCH_ALL_ROUTE_KEY.getKey());
        if (nonNull(routeConfigurationCatchAll)) {
            GlobalConfiguration.TenantConfiguration.RouteConfiguration.FunctionConfiguration functionConfiguration = routeConfigurationCatchAll.getFunction();
            if (nonNull(functionConfiguration)) {
                return Optional.of(getFunction(codeRepositoryUrl, functionConfiguration, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)));
            }
        }
        return Optional.empty();
    }


    private HandlerFunction<ServerResponse> getFunction(String codeServerURL, GlobalConfiguration.TenantConfiguration.RouteConfiguration.FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (HandlerFunction.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final HandlerFunction<ServerResponse> handlerFunction = (HandlerFunction<ServerResponse>) object;
                return handlerFunction;
            } else {
                throw new FunctionIsNotAHandlerFunctionException(String.format("Resource is not a HandlerFunction, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> getRoutePostFilters(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName,
            final String tenantId,
            final String routeId
    ) {
        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        final GlobalConfiguration.TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            return Collections.emptyList();
        }
        GlobalConfiguration.TenantConfiguration.RouteConfiguration routeConfiguration = tenantConfiguration.getRoutes().get(routeId);
        if (isNull(routeConfiguration)) {
            return Collections.emptyList();
        }
        return tenantConfiguration
                .getPost()
                .stream()
                .map(preFilter -> getPostFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)))
                .toList();
    }

    private List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> getTenantPostFilters(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName,
            final String tenantId
    ) {
        final GlobalConfiguration globalConfiguration;

        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        final GlobalConfiguration.TenantConfiguration tenantConfiguration = environmentConfiguration.getTenants().get(tenantId);
        if (isNull(tenantConfiguration)) {
            return Collections.emptyList();
        }
        return tenantConfiguration
                .getPost()
                .stream()
                .map(preFilter -> getPostFilter(codeRepositoryUrl, preFilter, String.format("environmentName=%s&tenantId=%s", environmentName, tenantId)))
                .toList();
    }

    private List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> getEnvironmentPostFilters(
            final Map<String, GlobalConfiguration.EnvironmentConfiguration> environments,
            final String codeRepositoryUrl,
            final String environmentName
    ) {
        final GlobalConfiguration.EnvironmentConfiguration environmentConfiguration = environments.get(environmentName);
        if (isNull(environmentConfiguration)) {
            return Collections.emptyList();
        }
        return environmentConfiguration
                .getPost()
                .stream()
                .map(postFilter -> getPostFilter(codeRepositoryUrl, postFilter, String.format("environmentName=%s", environmentName)))
                .toList();
    }

    private List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> getGlobalPostFilters(
            final List<GlobalConfiguration.PostFilterConfiguration> post,
            final String codeRepositoryUrl
    ) {
        return post
                .stream()
                .map(postFilterConfig -> getPostFilter(codeRepositoryUrl, postFilterConfig, "global"))
                .toList();
    }

    private BiFunction<ServerRequest, ServerResponse, ServerResponse> getPostFilter(String codeServerURL, GlobalConfiguration.PostFilterConfiguration configuration, String query) {
        String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (BiFunction.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final BiFunction<ServerRequest, ServerResponse, ServerResponse> filter = (BiFunction<ServerRequest, ServerResponse, ServerResponse>) object;
                return filter;
            } else {
                throw new PostFilterIsNotABiFunctionException(String.format("Resource is not a BiFunction, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }


    private ServerResponse getErrorResponse(Throwable t, ServerRequest request) {
        final HttpStatus httpStatus = request.attribute("RESPONSE_HTTP_STATUS")
                .map(code -> HttpStatus.valueOf(String.valueOf(code)))
                .orElse(HttpStatus.INTERNAL_SERVER_ERROR);
        final ServerResponse.BodyBuilder bodyBuilder = ServerResponse.status(httpStatus);
        ErrorDetail errorDetail = getErrorDetails(t);
        addErrorHeaders(errorDetail, bodyBuilder);
        logErrorDetails(errorDetail, request.attributes());
        return request.attribute("RESPONSE_MESSAGE")
                .map(bodyBuilder::body)
                .orElse(bodyBuilder.build());
    }

    private void logErrorDetails(final ErrorDetail errorDetail, final Map<String, Object> attributes) {
        final String errorMessage = String.format("%s %s", errorDetail.getErrorTypes(), errorDetail.getErrorMessages());
        assets.getLogger(attributes).error(errorMessage);
    }

    private ErrorDetail getErrorDetails(Throwable t) {
        final List<String> errorTypes = new ArrayList<>();
        final List<String> errorMessages = new ArrayList<>();
        errorTypes.add(t.getClass().getSimpleName());
        errorMessages.add(t.getMessage());
        Throwable cause = t.getCause();
        while (Objects.nonNull(cause)) {
            errorMessages.add(cause.getMessage());
            errorTypes.add(cause.getClass().getSimpleName());
            cause = cause.getCause();
        }
        return new ErrorDetail(errorTypes, errorMessages);
    }

    private void addErrorHeaders(ErrorDetail errorDetail, ServerResponse.BodyBuilder bodyBuilder) {
        bodyBuilder.header("X-A-Error-Type", String.join(", ", String.join(",", errorDetail.getErrorTypes())));
        bodyBuilder.header("X-A-Error-Message", String.join(", ", String.join(",", errorDetail.getErrorMessages())));
    }

}
