package io.archura.platform.function;

import com.fasterxml.jackson.databind.JsonNode;

public interface Configurable {
    void setConfiguration(JsonNode configuration);
}
