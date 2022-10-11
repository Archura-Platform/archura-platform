package io.archura.platform.internal;

import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.attribute.TenantKeys;
import io.archura.platform.api.exception.ErrorDetail;
import io.archura.platform.api.exception.FunctionIsNotAHandlerFunctionException;
import io.archura.platform.api.exception.PostFilterIsNotABiFunctionException;
import io.archura.platform.api.exception.PreFilterIsNotAUnaryOperatorException;
import io.archura.platform.api.exception.ResourceLoadException;
import io.archura.platform.api.http.HttpServerRequest;
import io.archura.platform.api.http.HttpServerResponse;
import io.archura.platform.api.http.HttpStatusCode;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.api.type.functionalcore.HandlerFunction;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.RequiredArgsConstructor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
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
    private final FilterFunctionExecutor filterFunctionExecutor;

    public HttpServerResponse handle(HttpServerRequest request) {
        try {
            final GlobalConfiguration globalConfiguration = GlobalConfiguration.getInstance();
            final String logLevel = globalConfiguration.getConfig().getLogLevel();
            final RedisCommands<String, String> redisCommands = globalConfiguration.getCacheConfiguration().getRedisCommands();

            final Map<String, Object> attributes = request.getAttributes();
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
            assets.buildContext(attributes, redisCommands);

            final List<UnaryOperator<HttpServerRequest>> globalPreFilters = getGlobalPreFilters(
                    globalConfiguration.getPre(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (UnaryOperator<HttpServerRequest> preFilter : globalPreFilters) {
                assets.getLogger(attributes).debug("Will run global PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, redisCommands);
            }

            String environmentName = String.valueOf(attributes.get(GlobalKeys.REQUEST_ENVIRONMENT.getKey()));
            final List<UnaryOperator<HttpServerRequest>> environmentPreFilters = getEnvironmentPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (UnaryOperator<HttpServerRequest> preFilter : environmentPreFilters) {
                assets.getLogger(attributes).debug("Will run environment PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, redisCommands);
            }
            /* REMOVE */
            attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.DEFAULT_TENANT_ID.getKey());
            assets.buildContext(attributes, redisCommands);

            String tenantId = String.valueOf(attributes.get(EnvironmentKeys.REQUEST_TENANT_ID.getKey()));
            final List<UnaryOperator<HttpServerRequest>> tenantPreFilters = getTenantPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (UnaryOperator<HttpServerRequest> preFilter : tenantPreFilters) {
                assets.getLogger(attributes).debug("Will run tenant PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, redisCommands);
            }

            final String routeId = String.valueOf(request.getAttributes().getOrDefault(TenantKeys.ROUTE_ID.getKey(), TenantKeys.CATCH_ALL_ROUTE_KEY.getKey()));
            final List<UnaryOperator<HttpServerRequest>> routePreFilters = getRoutePreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (UnaryOperator<HttpServerRequest> preFilter : routePreFilters) {
                assets.getLogger(attributes).debug("Will run route PreFilter: %s", preFilter.getClass().getSimpleName());
                request = filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, redisCommands);
            }

            final Optional<HandlerFunction<HttpServerResponse>> tenantFunctionOptional = getTenantFunctions(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            assets.getLogger(attributes).debug("Will run TenantFunction: %s", tenantFunctionOptional);

            HttpServerResponse response = new HttpServerResponse();
            if (tenantFunctionOptional.isPresent()) {
                final HandlerFunction<HttpServerResponse> tenantFunction = tenantFunctionOptional.get();
                response = filterFunctionExecutor.execute(request, tenantFunction);
            } else {
                response = response.toBuilder()
                        .status(HttpStatusCode.HTTP_NOT_FOUND)
                        .header("X-A-NotFound", String.format("%s-%s-%s", environmentName, tenantId, routeId))
                        .build();
            }

            final List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> routePostFilters = getRoutePostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> postFilter : routePostFilters) {
                assets.getLogger(attributes).debug("Will run route PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
            }

            final List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> tenantPostFilters = getTenantPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> postFilter : tenantPostFilters) {
                assets.getLogger(attributes).debug("Will run tenant PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
            }

            final List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> environmentPostFilters = getEnvironmentPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> postFilter : environmentPostFilters) {
                assets.getLogger(attributes).debug("Will run environment PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
            }
            final List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> globalPostFilters = getGlobalPostFilters(
                    globalConfiguration.getPost(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> postFilter : globalPostFilters) {
                assets.getLogger(attributes).debug("Will run global PostFilter: %s", postFilter.getClass().getSimpleName());
                response = filterFunctionExecutor.execute(request, response, postFilter);
            }
            return response;
        } catch (Exception e) {
            return this.getErrorResponse(e, request);
        }
    }

    private List<UnaryOperator<HttpServerRequest>> getGlobalPreFilters(
            final List<GlobalConfiguration.PreFilterConfiguration> preFilterConfigurations,
            final String codeRepositoryUrl
    ) {
        return preFilterConfigurations
                .stream()
                .map(preFilterConfig -> getPreFilter(codeRepositoryUrl, preFilterConfig, "global"))
                .toList();
    }

    private List<UnaryOperator<HttpServerRequest>> getEnvironmentPreFilters(
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

    private List<UnaryOperator<HttpServerRequest>> getTenantPreFilters(
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

    private List<UnaryOperator<HttpServerRequest>> getRoutePreFilters(
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

    private UnaryOperator<HttpServerRequest> getPreFilter(String codeServerURL, GlobalConfiguration.PreFilterConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (UnaryOperator.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final UnaryOperator<HttpServerRequest> consumer = (UnaryOperator<HttpServerRequest>) object;
                return consumer;
            } else {
                throw new PreFilterIsNotAUnaryOperatorException(String.format("Resource is not a UnaryOperator, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private Optional<HandlerFunction<HttpServerResponse>> getTenantFunctions(
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


    private HandlerFunction<HttpServerResponse> getFunction(String codeServerURL, GlobalConfiguration.TenantConfiguration.RouteConfiguration.FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (HandlerFunction.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final HandlerFunction<HttpServerResponse> handlerFunction = (HandlerFunction<HttpServerResponse>) object;
                return handlerFunction;
            } else {
                throw new FunctionIsNotAHandlerFunctionException(String.format("Resource is not a HandlerFunction, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> getRoutePostFilters(
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

    private List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> getTenantPostFilters(
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

    private List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> getEnvironmentPostFilters(
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

    private List<BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>> getGlobalPostFilters(
            final List<GlobalConfiguration.PostFilterConfiguration> post,
            final String codeRepositoryUrl
    ) {
        return post
                .stream()
                .map(postFilterConfig -> getPostFilter(codeRepositoryUrl, postFilterConfig, "global"))
                .toList();
    }

    private BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> getPostFilter(String codeServerURL, GlobalConfiguration.PostFilterConfiguration configuration, String query) {
        String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (BiFunction.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse> filter = (BiFunction<HttpServerRequest, HttpServerResponse, HttpServerResponse>) object;
                return filter;
            } else {
                throw new PostFilterIsNotABiFunctionException(String.format("Resource is not a BiFunction, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private HttpServerResponse getErrorResponse(Throwable t, HttpServerRequest request) {
        final HttpServerResponse httpResponse = new HttpServerResponse();
        try {
            final int statusCode = Integer.parseInt(String.valueOf(request.getAttributes().get("RESPONSE_HTTP_STATUS")));
            httpResponse.setStatus(statusCode);
        } catch (NumberFormatException exception) {
            httpResponse.setStatus(HttpStatusCode.HTTP_INTERNAL_ERROR);
            httpResponse.setBytes(exception.getMessage().getBytes(StandardCharsets.UTF_8));
        }

        final ErrorDetail errorDetail = getErrorDetails(t);
        httpResponse.setHeader("X-A-Error-Type", String.join(", ", String.join(",", errorDetail.getErrorTypes())));
        httpResponse.setHeader("X-A-Error-Message", String.join(", ", String.join(",", errorDetail.getErrorMessages())));

        final String errorMessage = String.format("%s %s", errorDetail.getErrorTypes(), errorDetail.getErrorMessages());
        final Logger logger = assets.getLogger(request.getAttributes());
        logger.error(errorMessage);

        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            final Object responseObject = request.getAttributes().getOrDefault("RESPONSE_MESSAGE", "");
            oos.writeObject(responseObject);
            final byte[] bytes = bos.toByteArray();
            httpResponse.setBytes(bytes);
        } catch (IOException exception) {
            httpResponse.setStatus(HttpStatusCode.HTTP_INTERNAL_ERROR);
            httpResponse.setBytes(exception.getMessage().getBytes(StandardCharsets.UTF_8));
        }

        return httpResponse;
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

}
