package io.pravega.connectors.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    @Override
    public byte[] serialize(T o) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            byte[] result = objectMapper.writeValueAsBytes(o);
            return result;
        } catch (Exception e) {
            return null;
        }
    }
}
