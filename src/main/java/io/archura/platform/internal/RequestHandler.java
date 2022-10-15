package io.archura.platform.internal;

import io.archura.platform.api.attribute.EnvironmentKeys;
import io.archura.platform.api.attribute.GlobalKeys;
import io.archura.platform.api.attribute.TenantKeys;
import io.archura.platform.api.exception.*;
import io.archura.platform.api.http.HttpServerRequest;
import io.archura.platform.api.http.HttpServerResponse;
import io.archura.platform.api.http.HttpStatusCode;
import io.archura.platform.api.logger.Logger;
import io.archura.platform.external.FilterFunctionExecutor;
import io.archura.platform.internal.cache.HashCache;
import io.archura.platform.internal.configuration.GlobalConfiguration;
import io.archura.platform.internal.publish.MessagePublisher;
import io.archura.platform.internal.stream.CacheStream;
import lombok.RequiredArgsConstructor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class RequestHandler {

    private final String configRepositoryUrl;
    private final HttpClient defaultHttpClient;
    private final Assets assets;
    private final FilterFunctionExecutor filterFunctionExecutor;

    public HttpServerResponse handle(final HttpServerRequest request) {
        try {
            final GlobalConfiguration globalConfiguration = GlobalConfiguration.getInstance();
            final String logLevel = globalConfiguration.getConfig().getLogLevel();
            final HashCache<String, String> hashCache = globalConfiguration.getCacheConfiguration().getHashCache();
            final CacheStream<String, Map<String, String>> cacheStream = globalConfiguration.getCacheConfiguration().getCacheStream();
            final MessagePublisher messagePublisher = globalConfiguration.getCacheConfiguration().getMessagePublisher();

            final Map<String, Object> attributes = request.getAttributes();
            attributes.put(GlobalKeys.REQUEST_LOG_LEVEL.getKey(), logLevel);
            assets.buildContext(attributes, hashCache, cacheStream, messagePublisher);

            final List<Consumer<HttpServerRequest>> globalPreFilters = getGlobalPreFilters(
                    globalConfiguration.getPre(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (Consumer<HttpServerRequest> preFilter : globalPreFilters) {
                assets.getLogger(attributes).debug("Will run global PreFilter: %s", preFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashCache, cacheStream, messagePublisher);
            }

            String environmentName = String.valueOf(attributes.get(GlobalKeys.REQUEST_ENVIRONMENT.getKey()));
            final List<Consumer<HttpServerRequest>> environmentPreFilters = getEnvironmentPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (Consumer<HttpServerRequest> preFilter : environmentPreFilters) {
                assets.getLogger(attributes).debug("Will run environment PreFilter: %s", preFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashCache, cacheStream, messagePublisher);
            }
            /* REMOVE */
            attributes.put(EnvironmentKeys.REQUEST_TENANT_ID.getKey(), EnvironmentKeys.DEFAULT_TENANT_ID.getKey());
            assets.buildContext(attributes, hashCache, cacheStream, messagePublisher);

            String tenantId = String.valueOf(attributes.get(EnvironmentKeys.REQUEST_TENANT_ID.getKey()));
            final List<Consumer<HttpServerRequest>> tenantPreFilters = getTenantPreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (Consumer<HttpServerRequest> preFilter : tenantPreFilters) {
                assets.getLogger(attributes).debug("Will run tenant PreFilter: %s", preFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashCache, cacheStream, messagePublisher);
            }

            final String routeId = String.valueOf(request.getAttributes().getOrDefault(TenantKeys.ROUTE_ID.getKey(), TenantKeys.CATCH_ALL_ROUTE_KEY.getKey()));
            final List<Consumer<HttpServerRequest>> routePreFilters = getRoutePreFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (Consumer<HttpServerRequest> preFilter : routePreFilters) {
                assets.getLogger(attributes).debug("Will run route PreFilter: %s", preFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, preFilter);
                assets.buildContext(attributes, hashCache, cacheStream, messagePublisher);
            }

            final Optional<Function<HttpServerRequest, HttpServerResponse>> tenantFunctionOptional = getTenantFunctions(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            assets.getLogger(attributes).debug("Will run TenantFunction: %s", tenantFunctionOptional);

            final HttpServerResponse response = new HttpServerResponse();
            if (tenantFunctionOptional.isPresent()) {
                final Function<HttpServerRequest, HttpServerResponse> tenantFunction = tenantFunctionOptional.get();
                HttpServerResponse httpServerResponse = filterFunctionExecutor.execute(request, tenantFunction);
                response.setStatus(httpServerResponse.getStatus());
                response.setBytes(httpServerResponse.getBytes());
                response.setHeaders(httpServerResponse.getHeaders());
            } else {
                response.setStatus(HttpStatusCode.HTTP_NOT_FOUND);
                response.setHeader("X-A-NotFound", String.format("%s-%s-%s", environmentName, tenantId, routeId));
            }

            final List<BiConsumer<HttpServerRequest, HttpServerResponse>> routePostFilters = getRoutePostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId,
                    routeId
            );
            for (BiConsumer<HttpServerRequest, HttpServerResponse> postFilter : routePostFilters) {
                assets.getLogger(attributes).debug("Will run route PostFilter: %s", postFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, response, postFilter);
            }

            final List<BiConsumer<HttpServerRequest, HttpServerResponse>> tenantPostFilters = getTenantPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName,
                    tenantId
            );
            for (BiConsumer<HttpServerRequest, HttpServerResponse> postFilter : tenantPostFilters) {
                assets.getLogger(attributes).debug("Will run tenant PostFilter: %s", postFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, response, postFilter);
            }

            final List<BiConsumer<HttpServerRequest, HttpServerResponse>> environmentPostFilters = getEnvironmentPostFilters(
                    globalConfiguration.getEnvironments(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl(),
                    environmentName
            );
            for (BiConsumer<HttpServerRequest, HttpServerResponse> postFilter : environmentPostFilters) {
                assets.getLogger(attributes).debug("Will run environment PostFilter: %s", postFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, response, postFilter);
            }
            final List<BiConsumer<HttpServerRequest, HttpServerResponse>> globalPostFilters = getGlobalPostFilters(
                    globalConfiguration.getPost(),
                    globalConfiguration.getConfig().getCodeRepositoryUrl()
            );
            for (BiConsumer<HttpServerRequest, HttpServerResponse> postFilter : globalPostFilters) {
                assets.getLogger(attributes).debug("Will run global PostFilter: %s", postFilter.getClass().getSimpleName());
                filterFunctionExecutor.execute(request, response, postFilter);
            }
            return response;
        } catch (Exception e) {
            return this.getErrorResponse(e, request);
        }
    }

    private List<Consumer<HttpServerRequest>> getGlobalPreFilters(
            final List<GlobalConfiguration.PreFilterConfiguration> preFilterConfigurations,
            final String codeRepositoryUrl
    ) {
        return preFilterConfigurations
                .stream()
                .map(preFilterConfig -> getPreFilter(codeRepositoryUrl, preFilterConfig, "global"))
                .toList();
    }

    private List<Consumer<HttpServerRequest>> getEnvironmentPreFilters(
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

    private List<Consumer<HttpServerRequest>> getTenantPreFilters(
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

    private List<Consumer<HttpServerRequest>> getRoutePreFilters(
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

    private Consumer<HttpServerRequest> getPreFilter(String codeServerURL, GlobalConfiguration.PreFilterConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (Consumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final Consumer<HttpServerRequest> consumer = (Consumer<HttpServerRequest>) object;
                return consumer;
            } else {
                throw new PreFilterIsNotAConsumerException(String.format("Resource is not a Consumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private Optional<Function<HttpServerRequest, HttpServerResponse>> getTenantFunctions(
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


    private Function<HttpServerRequest, HttpServerResponse> getFunction(String codeServerURL, GlobalConfiguration.TenantConfiguration.RouteConfiguration.FunctionConfiguration configuration, String query) {
        final String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (Function.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final Function<HttpServerRequest, HttpServerResponse> handlerFunction = (Function<HttpServerRequest, HttpServerResponse>) object;
                return handlerFunction;
            } else {
                throw new FunctionIsNotAFunctionException(String.format("Resource is not a Function, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private List<BiConsumer<HttpServerRequest, HttpServerResponse>> getRoutePostFilters(
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

    private List<BiConsumer<HttpServerRequest, HttpServerResponse>> getTenantPostFilters(
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

    private List<BiConsumer<HttpServerRequest, HttpServerResponse>> getEnvironmentPostFilters(
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

    private List<BiConsumer<HttpServerRequest, HttpServerResponse>> getGlobalPostFilters(
            final List<GlobalConfiguration.PostFilterConfiguration> post,
            final String codeRepositoryUrl
    ) {
        return post
                .stream()
                .map(postFilterConfig -> getPostFilter(codeRepositoryUrl, postFilterConfig, "global"))
                .toList();
    }

    private BiConsumer<HttpServerRequest, HttpServerResponse> getPostFilter(String codeServerURL, GlobalConfiguration.PostFilterConfiguration configuration, String query) {
        String resourceUrl = String.format("%s/%s-%s.jar", codeServerURL, configuration.getName(), configuration.getVersion());
        final String resourceKey = String.format("%s?%s", resourceUrl, query);
        try {
            final Object object = assets.createObject(resourceUrl, resourceKey, configuration.getName(), configuration.getConfig());
            if (BiConsumer.class.isAssignableFrom(object.getClass())) {
                @SuppressWarnings("unchecked") final BiConsumer<HttpServerRequest, HttpServerResponse> filter = (BiConsumer<HttpServerRequest, HttpServerResponse>) object;
                return filter;
            } else {
                throw new PostFilterIsNotABiConsumerException(String.format("Resource is not a BiConsumer, url: %s", resourceUrl));
            }
        } catch (Exception e) {
            throw new ResourceLoadException(e);
        }
    }

    private HttpServerResponse getErrorResponse(Throwable t, HttpServerRequest request) {
        final HttpServerResponse httpResponse = new HttpServerResponse();
        try {
            httpResponse.setBytes(t.getMessage().getBytes(StandardCharsets.UTF_8));
            final int statusCode = Integer.parseInt(String.valueOf(request.getAttributes().get("RESPONSE_HTTP_STATUS")));
            httpResponse.setStatus(statusCode);
        } catch (NumberFormatException exception) {
            httpResponse.setStatus(HttpStatusCode.HTTP_INTERNAL_ERROR);
        }

        final ErrorDetail errorDetail = getErrorDetails(t);
        httpResponse.setHeader("X-A-Error-Type", String.join(", ", String.join(",", errorDetail.getErrorTypes())));
        httpResponse.setHeader("X-A-Error-Message", String.join(", ", String.join(",", errorDetail.getErrorMessages())));

        final String errorMessage = String.format("%s %s", errorDetail.getErrorTypes(), errorDetail.getErrorMessages());
        final Logger logger = assets.getLogger(request.getAttributes());
        logger.error(errorMessage);

        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            final Object responseObject = request.getAttributes().get("RESPONSE_MESSAGE");
            if (nonNull(responseObject)) {
                oos.writeObject(responseObject);
                final byte[] bytes = bos.toByteArray();
                httpResponse.setBytes(bytes);
            }
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
