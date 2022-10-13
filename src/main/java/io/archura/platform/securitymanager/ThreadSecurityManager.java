package io.archura.platform.securitymanager;

import io.archura.platform.external.FilterFunctionExecutor;

import java.security.Permission;

public class ThreadSecurityManager extends SecurityManager {

    @Override
    public void checkPermission(Permission perm) {
        checkPermission(perm, null);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if ("java.net.SocketPermission".equals(perm.getClass().getName())) {
            checkSocketAccess(perm);
        } else if ("java.lang.RuntimePermission".equals(perm.getClass().getName())
                && "createSecurityManager".equals(perm.getName())) {
            checkReflectionAccess(perm);
        }
    }

    @Override
    public ThreadGroup getThreadGroup() {
        checkThreadPermission();
        return super.getThreadGroup();
    }

    private void checkSocketAccess(final Permission perm) {
        boolean isAllowedAPI = false;
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("io.archura.platform.internal")
                    || element.getClassName().equals("jdk.internal.net.http.HttpClientFacade")) {
                isAllowedAPI = true;
                continue;
            }
            if (!isAllowedAPI && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                throw new RuntimeException("Filters and Functions are not allowed to create sockets.");
            }
        }
    }

    private void checkReflectionAccess(Permission perm) {
        boolean isReflectionCall = false;
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("java.lang.reflect")) {
                isReflectionCall = true;
                continue;
            }
            if (isReflectionCall && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                throw new RuntimeException("Filters and Functions are not allowed to use reflection API.");
            }
        }
    }

    private void checkThreadPermission() {
        boolean isArchuraAPI = false;
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("io.archura.platform.internal")) {
                isArchuraAPI = true;
                continue;
            }
            if (!isArchuraAPI && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                throw new RuntimeException("Filters and Functions are not allowed to create Threads.");
            }
        }
    }

}