package message;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Weather {
        @JsonProperty("humidity")
        public int humidity;

        @JsonProperty("temperature")
        public int temperature;

        @JsonProperty("wind_speed")
        public int wind_speed;

        public Weather(int humidity, int temperature, int wind_speed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.wind_speed = wind_speed;
        }

        public Weather(){

        }
    }