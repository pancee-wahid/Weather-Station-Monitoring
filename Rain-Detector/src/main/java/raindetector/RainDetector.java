package raindetector;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;
import java.lang.*;


public class RainDetector {
    public static void main(String[] args) throws JsonProcessingException {
        // set up Kafka producer
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        // set up Kafka consumer
        Properties ConsumerProperties = new Properties();
        ConsumerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        ConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProperties.put("group.id", "rain-detection");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties);

        // subscribe to Kafka topic
        consumer.subscribe(Collections.singletonList("weather-messages"));

        ObjectMapper mapper = new ObjectMapper();

        // main loop
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> r : records) {
                Message message = mapper.readValue(r.value(), Message.class);

                if (message.weather.humidity > 70) {
                    ProducerRecord<String, String> record = new ProducerRecord<>("rain-detection", r.value());
                    producer.send(record);
                    System.out.println(r.value());
                }
            }
        }
    }
}