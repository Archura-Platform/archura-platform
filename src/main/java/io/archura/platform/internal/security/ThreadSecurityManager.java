package io.archura.platform.internal.security;

import io.archura.platform.external.FilterFunctionExecutor;
import java.security.Permission;

public class ThreadSecurityManager extends SecurityManager {

    @Override
    public void checkPermission(Permission perm) {
        checkSocketAccess(perm);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        checkSocketAccess(perm);
    }

    @Override
    public ThreadGroup getThreadGroup() {
        checkThreadPermission();
        return super.getThreadGroup();
    }

    private void checkSocketAccess(final Permission perm) {
        if (java.net.SocketPermission.class.isAssignableFrom(perm.getClass())) {
            boolean isAllowedAPI = false;
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                if (element.getClassName().startsWith("io.archura.platform.internal")
                || element.getClassName().equals("jdk.internal.net.http.HttpClientFacade")) {
                    isAllowedAPI = true;
                    continue;
                }
                if (!isAllowedAPI && element.getClassName().equals(FilterFunctionExecutor.class.getName())) {
                    throw new RuntimeException("Cannot create Sockets!");
                }
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
                throw new RuntimeException("Cannot create Threads!");
            }
        }
    }

}