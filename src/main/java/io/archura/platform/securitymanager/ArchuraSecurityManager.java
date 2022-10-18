package io.archura.platform.securitymanager;

import io.archura.platform.external.FilterFunctionExecutor;

import java.security.Permission;

public class ArchuraSecurityManager extends SecurityManager {

    @Override
    public void checkPermission(Permission perm) {
        checkPermission(perm, null);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if ("java.net.SocketPermission".equals(perm.getClass().getName())) {
            checkSocketAccess();
        } else if ("java.lang.RuntimePermission".equals(perm.getClass().getName())) {
            if ("createSecurityManager".equals(perm.getName())
                    || "accessDeclaredMembers".equals(perm.getName())) {
                checkReflectionAccess();
            } else if (perm.getName().startsWith("exitVM")) {
                throw new RuntimeException("Filters and Functions are not allowed to use restricted APIs.");
            }
        } else if ("java.lang.reflect.ReflectPermission".equals(perm.getClass().getName())
                && "suppressAccessChecks".equals(perm.getName())) {
            checkReflectionAccess();
        } else if ("java.io.FilePermission".equals(perm.getClass().getName())
                && "execute".equals(perm.getActions())) {
            checkFileAccess();
        }
    }

    @Override
    public ThreadGroup getThreadGroup() {
        checkThreadPermission();
        return super.getThreadGroup();
    }

    private void checkSocketAccess() {
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

    private void checkReflectionAccess() {
        boolean isAllowedAPI = false;
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("io.archura.platform.internal")
                    || element.getClassName().equals("com.fasterxml.jackson.databind.util.ClassUtil")) {
                isAllowedAPI = true;
                continue;
            }
            if (!isAllowedAPI && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                throw new RuntimeException("Filters and Functions are not allowed to use reflection API.");
            }
        }
    }

    private void checkFileAccess() {
        boolean isAllowedAPI = false;
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("io.archura.platform.internal")) {
                isAllowedAPI = true;
                continue;
            }
            if (!isAllowedAPI && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
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