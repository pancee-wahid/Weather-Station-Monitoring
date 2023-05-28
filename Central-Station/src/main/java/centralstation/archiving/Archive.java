package centralstation.archiving;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Archive {
    private String PARQUET_FILES_PATH;
    private int BATCH_SIZE;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat hourFormat = new SimpleDateFormat("HH-mm");
    private ObjectMapper mapper = new ObjectMapper();
    private Map<Long, Map<String, List<String>>> stationsBatches;
    private int collectedMessages;
    private Schema schema;

    public Archive(String PARQUET_FILES_PATH, int BATCH_SIZE) {
        this.PARQUET_FILES_PATH = PARQUET_FILES_PATH;
        this.BATCH_SIZE = BATCH_SIZE;
        this.collectedMessages = 0;
        // parse the schema
        schema = parseSchema();
        // store 10k messages temporarily to be written to parquet files as one batch
        stationsBatches = new HashMap<>();
        // Note: we can change stations ids to be any long number
        for (long i = 1; i <= 10; i++)
            stationsBatches.put(i, new HashMap<>());
    }

    private String getMapKey(long statusTimestamp) {
        Date date = new Date(statusTimestamp);
        String formattedTime = hourFormat.format(date);
        int minIndex = formattedTime.indexOf('-') + 1;
        int minute = Integer.parseInt(formattedTime.substring(minIndex));
        minute -= (minute % 5);
        String minString = minute < 10 ? "0" + minute : String.valueOf(minute);
        return dateFormat.format(date) + "__" + formattedTime.substring(0, minIndex) + minString;
    }


    private void writeToParquet(Map<Long, Map<String, List<String>>> stationsBatches, Schema schema) throws IOException {
        for (long i = 1; i <= 10 && !stationsBatches.get(i).isEmpty(); i++) {
            for (Map.Entry<String, List<String>> stationPartition : stationsBatches.get(i).entrySet()) {
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

    private Schema parseSchema() {
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

    private GenericData.Record generateRecord(Schema schema, Message message) throws JsonProcessingException {
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

    public void append(String value) throws IOException {
        Message message = mapper.readValue(value, Message.class);
        String key = getMapKey(message.status_timestamp);

        if (stationsBatches.get(message.station_id).containsKey(key)) {
            stationsBatches.get(message.station_id)
                    .get(key)
                    .add(value);
        } else {
            stationsBatches.get(message.station_id)
                    .put(key, new ArrayList<>());

            stationsBatches.get(message.station_id)
                    .get(key)
                    .add(value);

        }
        collectedMessages++;

        if (collectedMessages >= BATCH_SIZE) {
            System.out.println("Entering writeToParquet()");
            writeToParquet(stationsBatches, schema);
            for (long i = 1; i <= 10; i++)
                stationsBatches.get(i).clear();
            collectedMessages = 0;
        }
    }
}