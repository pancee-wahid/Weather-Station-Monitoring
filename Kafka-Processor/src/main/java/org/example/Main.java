package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.time.Duration;
import java.util.*;
import java.lang.*;

public class Main {
    public static void main(String[] args) {
        // Set up Kafka producer
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        // Set up Kafka consumer
        Properties ConsumerProperties = new Properties();
        ConsumerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        ConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProperties.put("group.id", "test-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties);

        // Subscribe to Kafka topic
        consumer.subscribe(Collections.singletonList("weather-messages"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> r : records) {
                int startIndex = r.value().indexOf("humidity") + 10;
                int endIndex = r.value().indexOf("," , startIndex);
                int humidity = Integer.parseInt(r.value().substring(startIndex, endIndex));

                // To-Do

            }
        }


    }
}