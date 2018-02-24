package io.pravega.example.taxidemo.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class JsonNodeSerializer implements Serializer<JsonNode>, Serializable {

    @Override
    public ByteBuffer serialize(JsonNode value) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            byte[] result = objectMapper.writeValueAsBytes(value);
            return ByteBuffer.wrap(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JsonNode deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readTree(bin);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
