package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
    @JsonProperty("station_id")
    long station_id;

    @JsonProperty("s_no")
    long s_no;

    @JsonProperty("battery_status")
    String battery_status;

    @JsonProperty("status_timestamp")
    long status_timestamp;
    @JsonProperty("weather")
    Weather weather;

    Message(long station_id, long s_no, String battery_status, long status_timestamp, int humidity, int temperature, int wind_speed) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = new Weather(humidity, temperature, wind_speed);

    }

    Message(){

    }
}