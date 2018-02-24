package io.pravega.example.taxidemo.gateway;

import java.net.URI;

class Parameters {
    public static URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER", "tcp://10.246.27.118:9091"));
    }

    public static String getScope() {
        return getEnvVar("PRAVEGA_SCOPE", "taxidemo5");
    }

    public static String getStreamName() {
        return getEnvVar("PRAVEGA_STREAM", "rawdata");
    }

    public static URI getGatewayURI() {
        return URI.create(getEnvVar("GATEWAY_URI", "http://localhost:3000/"));
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }
}
