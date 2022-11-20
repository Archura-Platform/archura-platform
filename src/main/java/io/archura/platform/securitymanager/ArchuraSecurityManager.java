package io.archura.platform.securitymanager;

import io.archura.platform.external.FilterFunctionExecutor;

import java.security.Permission;
import java.util.List;

public class ArchuraSecurityManager extends SecurityManager {

    public static final String REFLECTION_MESSAGE = "Filters and Functions are not allowed to use reflection API.";
    public static final String SOCKET_MESSAGE = "Filters and Functions are not allowed to create sockets.";
    public static final String RESTRICTED_MESSAGE = "Filters and Functions are not allowed to use restricted APIs.";
    public static final String CLASSLOADER_MESSAGE = "Filters and Functions are not allowed to create class loaders.";
    public static final String LIBRARY_MESSAGE = "Filters and Functions are not allowed to load libraries.";
    public static final String INTERNAL_PACKAGE_MESSAGE = "Filters and Functions are not allowed to access internal packages.";
    public static final String THREAD_MESSAGE = "Filters and Functions are not allowed to create Threads.";
    public static final String FILE_MESSAGE = "Filters and Functions are not allowed to do file operations.";
    public static final String INTERNAL_PACKAGE = "io.archura.platform.internal";
    public static final String SECURITY_MANAGER_PACKAGE = "io.archura.platform.securitymanager";
    private final List<String> allowedReflectionClasses = List.of("com.fasterxml.jackson.databind.util.ClassUtil");
    private final List<String> allowedSocketClasses = List.of("jdk.internal.net.http.HttpClientFacade");
    private final List<String> allowedThreadClasses = List.of();

    @Override
    public void checkPermission(Permission perm) {
        checkPermission(perm, null);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (perm instanceof java.net.SocketPermission) {
            checkAccess(SOCKET_MESSAGE, allowedSocketClasses);
        } else if (perm instanceof java.lang.RuntimePermission) {
            final String name = perm.getName();
            if ("accessDeclaredMembers".equals(name)) {
                checkAccess(REFLECTION_MESSAGE, allowedReflectionClasses);
            } else if ("createSecurityManager".equals(name) || name.startsWith("exitVM")) {
                throw new FilterFunctionSecurityException(RESTRICTED_MESSAGE);
            }
        } else if (perm instanceof java.lang.reflect.ReflectPermission && "suppressAccessChecks".equals(perm.getName())) {
            checkAccess(REFLECTION_MESSAGE, allowedReflectionClasses);
        } else if (perm instanceof java.io.FilePermission && ("execute".equals(perm.getActions()) || "write".equals(perm.getActions()) || "delete".equals(perm.getActions()))) {
            denyFilterFunctionAccess(FILE_MESSAGE);
        }
    }

    @Override
    public ThreadGroup getThreadGroup() {
        checkAccess(THREAD_MESSAGE, allowedThreadClasses);
        return super.getThreadGroup();
    }

    @Override
    public void checkCreateClassLoader() {
        denyFilterFunctionAccess(CLASSLOADER_MESSAGE);
        super.checkCreateClassLoader();
    }

    @Override
    public void checkLink(String lib) {
        denyFilterFunctionAccess(LIBRARY_MESSAGE);
        super.checkLink(lib);
    }

    @Override
    public void checkPackageAccess(String pkg) {
        if (pkg.startsWith(INTERNAL_PACKAGE) || pkg.startsWith(SECURITY_MANAGER_PACKAGE)) {
            denyFilterFunctionAccess(INTERNAL_PACKAGE_MESSAGE);
        }
        super.checkPackageAccess(pkg);
    }

    private void checkAccess(final String message, final List<String> allowedClasses) {
        boolean isAllowed = false;
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith(INTERNAL_PACKAGE) || allowedClasses.contains(element.getClassName())) {
                isAllowed = true;
                continue;
            }
            if (!isAllowed && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                throw new FilterFunctionSecurityException(message);
            }
        }
    }

    private void denyFilterFunctionAccess(final String message) {
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                throw new FilterFunctionSecurityException(message);
            }
        }
    }

}