package io.pravega.example.taxidemo.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class GatewayTest {
    private static final Logger Log = LoggerFactory.getLogger(GatewayTest.class);
    private static final int READER_TIMEOUT_MS = 2000;

    @Test
    public void testCreateStream() throws Exception {
        URI controllerURI = URI.create("tcp://10.246.27.118:9091");
        StreamManager streamManager = StreamManager.create(controllerURI);
        String scope = "taxidemo3";
        streamManager.createScope(scope);
        String streamName = "raw";
        int targetKBps = 1000;
        int scaleFactor = 2;
        int minNumSegments = 1;
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byDataRate(targetKBps, scaleFactor, minNumSegments))
                .build();
        streamManager.createStream(scope, streamName, streamConfig);

        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        EventStreamWriter<JsonNode> writer = clientFactory.createEventWriter(
                streamName,
                new JsonNodeSerializer(),
                EventWriterConfig.builder().build());

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode message = objectMapper.createObjectNode();
        String routingKeyAttributeName = "key1";
        message.put(routingKeyAttributeName, "myroutingkey1");
        message.put("key2", "value2");
        message.put("key3", "value3");
        JsonNode routingKeyNode = message.get(routingKeyAttributeName);
        String routingKey = objectMapper.writeValueAsString(routingKeyNode);
        Log.info("routingKey={}", routingKey);
        final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
        writeFuture.get();
        Log.info("done writing");

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig, Collections.singleton(streamName));
        }

        try (EventStreamReader<JsonNode> reader = clientFactory.createReader("reader",
                     readerGroup,
                     new JsonNodeSerializer(),
                     ReaderConfig.builder().build())) {
            Log.info("Reading all the events from {}/{}", scope, streamName);
            EventRead<JsonNode> event = null;
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    if (event.getEvent() != null) {
                        Log.info("Read event {}", event.getEvent());
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    Log.error(e.toString());
                }
            } while (event.getEvent() != null);
            Log.info("No more events from {}/{}", scope, streamName);
        }

    }
}
