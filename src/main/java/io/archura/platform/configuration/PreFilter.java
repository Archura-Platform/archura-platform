package io.archura.platform.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
public class PreFilter {
    private String name;
    private String version;
    private JsonNode config;
}
