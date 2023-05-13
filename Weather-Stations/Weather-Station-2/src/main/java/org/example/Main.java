package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.lang.*;

public class Main {
    private static final int stationId = 2;
    private static long s_no = 0;

    public static void main(String[] args) throws InterruptedException {
        // set up Kafka producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // random instance to be used in random numbers generation
        Random random = new Random();

        // main loop
        while (true) {
            // shuffling battery status to have randomly 30% low, 40% medium, 30% high
            // these values correspond to the 10 messages that will be created in this iteration of while loop
            String[] batteryStatus = {"low", "low", "low", "medium", "medium", "medium", "medium", "high", "high", "high"};
            List<String> shuffledStatus = Arrays.asList(batteryStatus);
            Collections.shuffle(shuffledStatus);

            // random number from 0 to 9 to select a random message to be dropped of the 10 messages
            int rand = random.nextInt(1000000) % 10;

            // creating the 10 messages
            for (int i = 0; i < 10; i++) {
                // self-incremented sequence number of te messages
                s_no++;

                // if this is the message to be dropped, skip this iteration
                if (i == rand)
                    continue;

                // generate random values for humidity, temperature and wind speed
                int humidity = random.nextInt(101);
                int temperature = random.nextInt(150);
                int windSpeed = random.nextInt(150 - 5) + 5;

                // create the message
                Message msg = new Message(stationId, s_no, shuffledStatus.get(i), System.currentTimeMillis(), humidity, temperature, windSpeed);

                //convert the message from Message Object to Json String
                Gson gson = new Gson();
                String json = gson.toJson(msg);
                System.out.println(json); // for debugging

                // produce the message on the topic "weather-messages"
                ProducerRecord<String, String> record = new ProducerRecord<>("weather-messages", json);
                producer.send(record);

                // sleep after producing each message
                Thread.sleep(1000);
            }
        }
    }
}