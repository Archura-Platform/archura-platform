package io.archura.platform.internal.stream;

import io.archura.platform.api.stream.LightStream;
import jdk.internal.reflect.Reflection;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StreamOperations;

import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
public class TenantStream implements LightStream {

    static {
        Reflection.registerFieldsToFilter(TenantStream.class, Set.of("tenantKey", "streamOperations"));
    }

    private final String tenantKey;
    private final StreamOperations<String, Object, Object> streamOperations;

    public Optional<String> send(final String topicName, final byte[] value) {
        final String streamKey = String.format("%s-%s", tenantKey, topicName);
        final ObjectRecord<String, byte[]> streamRecord = StreamRecords.newRecord()
                .ofObject(value)
                .withStreamKey(streamKey);
        final RecordId recordId = streamOperations.add(streamRecord);
        return Optional.ofNullable(recordId).map(RecordId::getValue);
    }

}
