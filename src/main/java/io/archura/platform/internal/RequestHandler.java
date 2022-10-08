package io.archura.platform.internal;

import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.attribute.TenantKeys;
import io.archura.platform.api.exception.ErrorDetail;
import io.archura.platform.api.exception.FunctionIsNotAHandlerFunctionException;
import io.archura.platform.api.exception.PostFilterIsNotABiFunctionException;
import io.archura.platform.api.exception.PreFilterIsNotAUnaryOperatorException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    private final FilterFunctionExecutor filterFunctionExecutor;

    public ServerResponse handle(ServerRequest request) {
        try {
            final GlobalConfiguration globalConfiguration = beanFactory.getBean(GlobalConfiguration.class);
            final String logLevel = globalConfiguration.getConfig().getLogLevel();
            final HashOperations<String, String, Map<String, Object>> hashOperations = globalConfiguration.getCacheConfiguration().getHashOperations();
            final StreamOperations<String, Object, Object> streamOperations = globalConfiguration.getCacheConfiguration().getStreamOperations();

            final Map<String, Object> attributes = request.attributes();
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
            assets.buildContext(attributes, hashOperations, streamOperations);

            final List<UnaryOperator<ServerRequest>> globalPreFilters = getGlobalPreFilters(
                    globalConfiguration.getPre(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (UnaryOperator<ServerRequest> preFilter : globalPreFilters) {
                assets.getLogger(attributes).debug("Will run global PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashOperations, streamOperations);
            }

            String environmentName = String.valueOf(attributes.get(GlobalKeys.REQUEST_ENVIRONMENT.getKey()));
            final List<UnaryOperator<ServerRequest>> environmentPreFilters = getEnvironmentPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (UnaryOperator<ServerRequest> preFilter : environmentPreFilters) {
                assets.getLogger(attributes).debug("Will run environment PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashOperations, streamOperations);
            }
            /* REMOVE */
            attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.DEFAULT_TENANT_ID.getKey());
            assets.buildContext(attributes, hashOperations, streamOperations);

            String tenantId = String.valueOf(attributes.get(EnvironmentKeys.REQUEST_TENANT_ID.getKey()));
            final List<UnaryOperator<ServerRequest>> tenantPreFilters = getTenantPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (UnaryOperator<ServerRequest> preFilter : tenantPreFilters) {
                assets.getLogger(attributes).debug("Will run tenant PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashOperations, streamOperations);
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
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashOperations, streamOperations);
            }

            final Optional<HandlerFunction<ServerResponse>> tenantFunctionOptional = getTenantFunctions(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            assets.getLogger(attributes).debug("Will run TenantFunction: %s", tenantFunctionOptional);

            ServerResponse response;
            if (tenantFunctionOptional.isPresent()) {
                final HandlerFunction<ServerResponse> tenantFunction = tenantFunctionOptional.get();
                response = filterFunctionExecutor.execute(request, tenantFunction);
            } else {
                response = ServerResponse
                        .notFound()
                        .header(String.format("X-A-NotFound-%s-%s-%s", environmentName, tenantId, routeId))
                        .build();
            }

            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> routePostFilters = getRoutePostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : routePostFilters) {
                assets.getLogger(attributes).debug("Will run route PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute
                        (request, response, postFilter);
            }

            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> tenantPostFilters = getTenantPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : tenantPostFilters) {
                assets.getLogger(attributes).debug("Will run tenant PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
            }

            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> environmentPostFilters = getEnvironmentPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : environmentPostFilters) {
                assets.getLogger(attributes).debug("Will run environment PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
            }
            final List<BiFunction<ServerRequest, ServerResponse, ServerResponse>> globalPostFilters = getGlobalPostFilters(
                    globalConfiguration.getPost(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (BiFunction<ServerRequest, ServerResponse, ServerResponse> postFilter : globalPostFilters) {
                assets.getLogger(attributes).debug("Will run global PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
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
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig(), configuration.isReload());
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
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig(), configuration.isReload());
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
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig(), configuration.isReload());
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
