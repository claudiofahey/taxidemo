package io.pravega.example.taxidemo.flinkprocessor;

import java.io.Serializable;

public class GeoPoint implements Serializable {
    public double lat;
    public double lon;

    @Override
    public String toString() {
        return "GeoPoint{" +
                "lat=" + lat +
                ", lon=" + lon +
                '}';
    }
}
