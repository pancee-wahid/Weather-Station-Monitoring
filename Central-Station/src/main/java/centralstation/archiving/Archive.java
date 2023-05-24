package centralstation.archiving;
import message.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import centralstation.bitcask.Bitcask_Old;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Archive {
    final static String PARQUET_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\Central-Station\\parquet-files\\";
    final static int BATCH_SIZE = 10000;
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat hourFormat = new SimpleDateFormat("HH-mm");

    // object mapper will be used to get Message object from json string
    static ObjectMapper mapper = new ObjectMapper();

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

        // parse the schema
        Schema schema = parseSchema();

        // Bitcask
        Bitcask_Old bitcask = new Bitcask_Old();

        // ToDo : change the list to be map from station id to map of time->list_of_msgs_at_this_time
        // store 10k messages temporarily to be written to parquet files as one batch
        List<Map<String, List<String>>> stationsBatches = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            stationsBatches.add(new HashMap<>());

        int sum = 0;

        // main loop
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> r : records) {
                if (!r.value().startsWith("{\"station_id\""))
                    break;
                Message message = mapper.readValue(r.value(), Message.class);
                String key = getMapKey(message.status_timestamp);

                bitcask.append(message.station_id, r.value());
                if (stationsBatches.get((int) message.station_id - 1).containsKey(key)) {
                    stationsBatches.get((int) message.station_id - 1)
                            .get(key)
                            .add(r.value());
                } else {
                    stationsBatches.get((int) message.station_id - 1)
                            .put(key, new ArrayList<>());

                    stationsBatches.get((int) message.station_id - 1)
                            .get(key)
                            .add(r.value());

                }
                sum++;

                if (sum >= BATCH_SIZE) {
                    System.out.println("Entering writeToParquet()");
                    writeToParquet(stationsBatches, schema);
                    for (int i = 0; i < 10; i++)
                        stationsBatches.get(i).clear();
                    sum = 0;
                }
            }
        }
    }

    private static String getMapKey(long statusTimestamp) {
        Date date = new Date(statusTimestamp);
        String formattedTime = hourFormat.format(date);
        int minIndex = formattedTime.indexOf('-') + 1;
        int minute = Integer.parseInt(formattedTime.substring(minIndex));
        minute -= (minute % 5);
        String minString = minute < 10 ? "0" + minute : String.valueOf(minute);
        return dateFormat.format(date) + "__" + formattedTime.substring(0, minIndex) + minString;
    }


    private static void writeToParquet(List<Map<String, List<String>>> stationsBatches, Schema schema) throws IOException {
        for (int i = 1; i <= 10 && !stationsBatches.get(i - 1).isEmpty(); i++) {
            for (Map.Entry<String, List<String>> stationPartition : stationsBatches.get(i - 1).entrySet()) {
                // get the path of the file to write to
                String filePath = PARQUET_FILES_PATH + "s" + i + "\\s" + i + "__" + stationPartition.getKey() + "__p0" + ".parquet";
                File file = new File(filePath);
                System.out.println(filePath);
                while (file.exists()) {
                    filePath = filePath.substring(0, filePath.indexOf("__p") + 3)
                            + (Integer.parseInt(filePath.substring(filePath.indexOf("__p") + 3, filePath.indexOf(".parquet"))) + 1)
                            + ".parquet";
                    file = new File(filePath);
                }
                System.out.println(filePath);
                Path path = new Path(filePath);

                // write to (station i with specified time) parquet file
                try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                        .withSchema(schema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                        .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                        .withConf(new Configuration())
                        .build()) {

                    for (String msg : stationPartition.getValue()) {
                        // create the Message object from the json string
                        Message message = mapper.readValue(msg, Message.class);
                        // generate the parquet record
                        GenericData.Record record = generateRecord(schema, message);
                        // write the Avro record to the Parquet file
                        writer.write(record);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write records to Parquet file: " + e.getMessage(), e);
                }
            }
        }
    }

    private static Schema parseSchema() {
        String schemaJson = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"WeatherStationMessage\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"station_id\", \"type\": \"long\"},\n" +
                "    {\"name\": \"s_no\", \"type\": \"long\"},\n" +
                "    {\"name\": \"battery_status\", \"type\": \"string\"},\n" +
                "    {\"name\": \"status_timestamp\", \"type\": \"long\"},\n" +
                "    {\n" +
                "      \"name\": \"weather\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"record\",\n" +
                "        \"name\": \"Weather\",\n" +
                "        \"fields\": [\n" +
                "          {\"name\": \"humidity\", \"type\": \"int\"},\n" +
                "          {\"name\": \"temperature\", \"type\": \"int\"},\n" +
                "          {\"name\": \"wind_speed\",\"type\": \"int\"}\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    private static GenericData.Record generateRecord(Schema schema, Message message) throws JsonProcessingException {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("station_id", message.station_id);
        record.put("s_no", message.s_no);
        record.put("battery_status", message.battery_status);
        record.put("status_timestamp", message.status_timestamp);
        Schema weatherSchema = schema.getField("weather").schema();
        GenericData.Record weatherRecord = new GenericData.Record(weatherSchema);
        weatherRecord.put("humidity", message.weather.humidity);
        weatherRecord.put("temperature", message.weather.temperature);
        weatherRecord.put("wind_speed", message.weather.wind_speed);
        record.put("weather", weatherRecord);
        return record;
    }
}