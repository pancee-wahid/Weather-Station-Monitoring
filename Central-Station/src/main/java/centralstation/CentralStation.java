package centralstation;

import centralstation.archiving.Archive;
import centralstation.bitcask.Bitcask;
import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CentralStation {
    private static final String BITCASK_LOG_PATH = "/home/mohamed-yasser/Projects/Weather Station/Weather-Station-Monitoring/Central-Station/bitcask/";
    private static final int MAX_LOG_FILE_SIZE = 100000; // 0.5 MB
    private static final int MAX_LOG_FILE_COUNT = 2; // maximum number of log files to keep before starting compaction
    private static final int NUM_OF_STATIONS = 10;
    private static final String PARQUET_FILES_PATH = "/home/mohamed-yasser/Projects/Weather Station/Weather-Station-Monitoring/Central-Station/parquet-files-3/";
    private static final int BATCH_SIZE = 10000;

    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        // set up Kafka consumer
        Properties ConsumerProperties = new Properties();
        ConsumerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        ConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProperties.put("group.id", "test-group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConsumerProperties);

        // subscribe to Kafka topic
        consumer.subscribe(Collections.singletonList("weather-messages"));

        Archive archive = new Archive(PARQUET_FILES_PATH, BATCH_SIZE);
        Bitcask bitcask = new Bitcask(BITCASK_LOG_PATH, MAX_LOG_FILE_SIZE, MAX_LOG_FILE_COUNT, NUM_OF_STATIONS);

        // main loop
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> r : records) {
                if (!r.value().startsWith("{\"station_id\""))
                    break;
                Message message = mapper.readValue(r.value(), Message.class);
                archive.append(r.value());
                bitcask.append(message.station_id, r.value());
            }
        }
    }

}

