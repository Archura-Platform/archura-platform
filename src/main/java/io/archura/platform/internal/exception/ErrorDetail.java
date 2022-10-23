package io.archura.platform.internal.exception;

import java.util.List;

public class ErrorDetail {
    private final List<String> errorTypes;
    private final List<String> errorMessages;

    public ErrorDetail(List<String> errorTypes, List<String> errorMessages) {
        this.errorTypes = errorTypes;
        this.errorMessages = errorMessages;
    }

    public List<String> getErrorTypes() {
        return errorTypes;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }
}
