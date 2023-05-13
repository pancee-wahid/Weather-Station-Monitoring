package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.lang.*;

public class Main {
    private static final int stationId = 1;
    private static long s_no = 0;

    public static void main(String[] args) throws InterruptedException {
        // Set up Kafka producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Random random = new Random();

        while (true) {
            String[] batteryStatus = {"low", "low", "low", "medium", "medium", "medium", "medium", "high", "high", "high"};
            List<String> shuffledStatus = Arrays.asList(batteryStatus);
            Collections.shuffle(shuffledStatus);

            int rand = random.nextInt(1000000) % 10;
            for (int i = 0; i < 10; i++) {
                s_no++;
                if (i == rand)
                    continue;
                int humidity = random.nextInt(101);
                int temperature = random.nextInt(150);
                int windSpeed = random.nextInt(150 - 5) + 5;
                Message msg = new Message(stationId, s_no, shuffledStatus.get(i), System.currentTimeMillis(), humidity, temperature, windSpeed);
                Gson gson = new Gson();
                String json = gson.toJson(msg);
                System.out.println(json);
                ProducerRecord<String, String> record = new ProducerRecord<>("weather-messages", json);
                producer.send(record);
                Thread.sleep(1000);
            }
        }
    }
}