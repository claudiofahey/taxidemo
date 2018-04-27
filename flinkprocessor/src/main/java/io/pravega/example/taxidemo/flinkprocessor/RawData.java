package io.pravega.example.taxidemo.flinkprocessor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RawData implements Serializable {
    public String event_type;
    public String trip_id;
    public double trip_fraction;
    public double trip_duration_minutes;
    public int VendorID;
    public long timestamp;
    public int passenger_count;
    public double trip_distance;
    public GeoPoint location;
    public int RateCodeID;
    public String store_and_fwd_flag;
    public int payment_type;
    public double fare_amount;
    public double extra;
    public double mta_tax;
    public double tip_amount;
    public double tolls_amount;
    public double improvement_surcharge;
    public double total_amount;

    @Override
    public String toString() {
        return "RawData{" +
                "event_type='" + event_type + '\'' +
                ", trip_id='" + trip_id + '\'' +
                ", trip_fraction=" + trip_fraction +
                ", trip_duration_minutes=" + trip_duration_minutes +
                ", VendorID=" + VendorID +
                ", timestamp=" + timestamp +
                ", passenger_count=" + passenger_count +
                ", trip_distance=" + trip_distance +
                ", location=" + location +
                ", RateCodeID=" + RateCodeID +
                ", store_and_fwd_flag='" + store_and_fwd_flag + '\'' +
                ", payment_type=" + payment_type +
                ", fare_amount=" + fare_amount +
                ", extra=" + extra +
                ", mta_tax=" + mta_tax +
                ", tip_amount=" + tip_amount +
                ", tolls_amount=" + tolls_amount +
                ", improvement_surcharge=" + improvement_surcharge +
                ", total_amount=" + total_amount +
                '}';
    }
}
