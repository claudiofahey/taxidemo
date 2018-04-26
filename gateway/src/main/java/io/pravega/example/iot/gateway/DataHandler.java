package io.pravega.example.iot.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import org.glassfish.grizzly.http.server.Request;
import java.util.concurrent.CompletableFuture;

@Path("data")
public class DataHandler {
    private static final Logger Log = LoggerFactory.getLogger(DataHandler.class);

    @POST
    @Consumes({"application/json"})
    @Produces({"application/json"})
    public String postData(@Context Request request, String data) throws Exception {
        // Deserialize the JSON message.
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode message = (ObjectNode) objectMapper.readTree(data);

        // Add the remote IP address to JSON message.
        // TODO: Make this optional.
        String remoteAddr = request.getRemoteAddr();
        message.put("remote_addr", remoteAddr);

        // Get or calculate the routing key.
        String routingKeyAttributeName = Parameters.getRoutingKeyAttributeName();
        String routingKey;
        if (routingKeyAttributeName.isEmpty()) {
            // TODO: Would it be better to use an empty routing key?
            routingKey = Double.toString(Math.random());
        } else {
            JsonNode routingKeyNode = message.get(routingKeyAttributeName);
            routingKey = objectMapper.writeValueAsString(routingKeyNode);
        }

        // Write the message to Pravega.
        Log.info("routingKey={}, message={}", routingKey, message);
        final CompletableFuture writeFuture = Main.getWriter().writeEvent(routingKey, message);

        // Wait for acknowledgement that the event was durably persisted.
        // TODO: Wait should be an option that can be set by each request.
        writeFuture.get();

        return "{}";
    }
}
