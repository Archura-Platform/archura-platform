package io.archura.platform.internal.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.archura.platform.api.mapper.Mapper;

import java.io.IOException;

public class ObjectMapper implements Mapper {
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;

    public ObjectMapper(final com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        this.mapper = objectMapper;
    }

    @Override
    public String writeValueAsString(Object object) throws JsonProcessingException {
        return this.mapper.writeValueAsString(object);
    }

    @Override
    public <T> T readValue(byte[] bytes, Class<T> tClass) throws IOException {
        return this.mapper.readValue(bytes, tClass);
    }

    @Override
    public <T> T convertValue(Object fromValue, Class<T> toValueType) throws IllegalArgumentException {
        return this.mapper.convertValue(fromValue, toValueType);
    }

}
