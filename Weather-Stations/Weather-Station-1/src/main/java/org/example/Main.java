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
                ProducerRecord<String, String> record = new ProducerRecord<>("my_first", json);
                producer.send(record);
                Thread.sleep(1000);
            }
        }
    }
}


/*
public class Kafka_2 {

    public static void main(String[] args) throws Exception{

        // Set up Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", "test-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        // Subscribe to Kafka topic
        consumer.subscribe(Collections.singletonList("lab4-latency2"));

        Long start;
        for (int i = 0; i < differences.length; i++) {
            // Send file contents to Kafka topic
            ProducerRecord<String, String> record = new ProducerRecord<>("lab4-latency2", Long.toString(System.currentTimeMillis()), stringBuilder.toString());
            start = System.currentTimeMillis();
            producer.send(record);
            System.out.println("Produce response time " + (System.currentTimeMillis() - start) + "ms");
            // Receive messages from Kafka topic
            start = System.currentTimeMillis();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            System.out.println("Consume response time " + (System.currentTimeMillis() - start) + "ms");
            for (ConsumerRecord<String, String> r : records) {
                differences[i] = System.currentTimeMillis() - Long.parseLong(r.key());
            }
        }
        Arrays.sort(differences);
        System.out.println("The Median Latency is: " + differences[4999] + "ms");
    }
}



 */