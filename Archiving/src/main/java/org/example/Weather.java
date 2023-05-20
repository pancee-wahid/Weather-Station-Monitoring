package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

class Weather {
        @JsonProperty("humidity")
        int humidity;

        @JsonProperty("temperature")
        int temperature;

        @JsonProperty("wind_speed")
        int wind_speed;

        Weather(int humidity, int temperature, int wind_speed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.wind_speed = wind_speed;
        }

        Weather(){

        }
    }