package org.example;

public class Message {
    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;

    class Weather {
        int humidity;
        int temperature;
        int wind_speed;
        Weather(int humidity, int temperature, int wind_speed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.wind_speed = wind_speed;
        }
    }

    Weather weather;

    Message(long station_id, long s_no, String battery_status, long status_timestamp, int humidity, int temperature, int wind_speed) {
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = new Weather(humidity, temperature, wind_speed);

    }
}
