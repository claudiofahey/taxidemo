package io.pravega.example.taxidemo.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import org.glassfish.grizzly.http.server.Request;
import java.util.concurrent.CompletableFuture;

@Path("data")
public class SensorDataHandler {
    private static final Logger Log = LoggerFactory.getLogger(SensorDataHandler.class);

    @GET
    public String request1(@Context Request request) throws Exception {
        String remoteAddr = request.getRemoteAddr();

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode message = objectMapper.createObjectNode();
        String routingKeyAttributeName = "key1";
        message.put(routingKeyAttributeName, "myroutingkey1");
        message.put("key2", "value2");
        message.put("key3", "value3");
        message.put("remoteAddr", remoteAddr);
        JsonNode routingKeyNode = message.get(routingKeyAttributeName);
        String routingKey = objectMapper.writeValueAsString(routingKeyNode);
        Log.info("routingKey={}", routingKey);
        Log.info("message={}", message);
        final CompletableFuture writeFuture = Main.getWriter().writeEvent(routingKey, message);
        writeFuture.get();
        Log.info("done writing");

        return "This is request1";
    }
}
