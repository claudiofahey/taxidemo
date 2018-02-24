package io.pravega.example.taxidemo.gateway;

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
public class SensorDataHandler {
    private static final Logger Log = LoggerFactory.getLogger(SensorDataHandler.class);

    @POST
    @Consumes({"application/json"})
    @Produces({"application/json"})
    public String postData(@Context Request request, String data) throws Exception {
        String remoteAddr = request.getRemoteAddr();

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode message = objectMapper.createObjectNode();

        String routingKeyAttributeName = "";
        String routingKey;
        if (routingKeyAttributeName.isEmpty()) {
            routingKey = Double.toString(Math.random());
        } else {
            JsonNode routingKeyNode = message.get(routingKeyAttributeName);
            routingKey = objectMapper.writeValueAsString(routingKeyNode);
        }

        message.put("data", data);
        message.put("remoteAddr", remoteAddr);
        Log.info("routingKey={}, message={}", routingKey, message);

        final CompletableFuture writeFuture = Main.getWriter().writeEvent(routingKey, message);
//        writeFuture.get();
//        Log.info("done writing");
        return "{}";
    }
}
