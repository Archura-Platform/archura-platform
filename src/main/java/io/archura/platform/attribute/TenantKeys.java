package io.archura.platform.attribute;

public enum TenantKeys {

    ROUTE_ID("ARCHURA_ROUTE_ID"),
    CATCH_ALL_ROUTE_KEY("*");

    private final String key;

    TenantKeys(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}
