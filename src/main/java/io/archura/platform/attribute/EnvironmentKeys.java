package io.archura.platform.attribute;

public enum EnvironmentKeys {

    TENANT_NOT_SET("NO_TENANT"),
    DEFAULT_TENANT_ID("DEFAULT"),
    REQUEST_TENANT_ID("ARCHURA_REQUEST_TENANT_ID");

    private final String key;

    EnvironmentKeys(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}
