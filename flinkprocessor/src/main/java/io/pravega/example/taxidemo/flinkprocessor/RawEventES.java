package io.pravega.example.taxidemo.flinkprocessor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RawEventES implements Serializable {
    public long timestamp;

    @JsonProperty("dropoff_latitude")
    public double dropoffLatitude;

    @JsonProperty("dropoff_longitude")
    public double dropoffLongitude;

    @Override
    public String toString() {
        return "RawEventES{" +
                "timestamp=" + timestamp +
                ", dropoffLatitude=" + dropoffLatitude +
                ", dropoffLongitude=" + dropoffLongitude +
                '}';
    }
}
