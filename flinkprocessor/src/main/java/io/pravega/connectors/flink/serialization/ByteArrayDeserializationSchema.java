package io.pravega.connectors.flink.serialization;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class ByteArrayDeserializationSchema extends AbstractDeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        return message;
    }
}
