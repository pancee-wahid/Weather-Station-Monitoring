package centralstation.bitcask;

import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;

import java.io.*;
import java.util.*;

public class Bitcask {

    private int MAX_LOG_FILE_SIZE;
    private int MAX_LOG_FILE_COUNT;
    private int NUM_OF_STATIONS;
    private String BITCASK_LOG_PATH;
    private final String HINT_FILE_PREFIX = "hint_";
    private final String LOG_FILE_PREFIX = "log_";
    private final String FILES_EXTENSION = ".bin";
    private String currentLogName;
    private long currentByte;
    private int numberOfCurrentLogs;
    private ObjectMapper objectMapper = new ObjectMapper();
    // Long: station_id , Location: location of its most recent value
    private Map<Long, RecentLocation> inMemoryHashmap;


    public Bitcask(String BITCASK_LOG_PATH, int MAX_LOG_FILE_SIZE, int MAX_LOG_FILE_COUNT, int NUM_OF_STATIONS) {
        this.BITCASK_LOG_PATH = BITCASK_LOG_PATH;
        this.MAX_LOG_FILE_SIZE = MAX_LOG_FILE_SIZE;
        this.MAX_LOG_FILE_COUNT = MAX_LOG_FILE_COUNT;
        this.NUM_OF_STATIONS = NUM_OF_STATIONS;
        inMemoryHashmap = new HashMap<>();
        currentByte = 0;
        numberOfCurrentLogs = 0;
        currentLogName = "";
    }

    private class RecentLocation {
        long fileID;
        short valueSize;
        long valueOffset;
        long timestamp;

        RecentLocation(long timestamp, long fileID, short valueSize, long valueOffset) {
            this.timestamp = timestamp;
            this.fileID = fileID;
            this.valueSize = valueSize;
            this.valueOffset = valueOffset;
        }
    }

    private void readHintFile(String hintFilePath) throws IOException {
        long stationID, timestamp, fileID, valueOffset;
        short valueSize;
        try (FileInputStream fis = new FileInputStream(hintFilePath)) {
            DataInputStream dis = new DataInputStream(fis);
            while (dis.available() > 0) {
                stationID = dis.readLong();
                timestamp = dis.readLong();
                fileID = dis.readLong();
                valueSize = dis.readShort();
                valueOffset = dis.readLong();
                if (inMemoryHashmap.containsKey(stationID) && inMemoryHashmap.get(stationID).timestamp > timestamp)
                    continue;
                inMemoryHashmap.put(stationID,  new RecentLocation(timestamp, fileID, valueSize, valueOffset));
            }
            dis.close();
        }
    }

    private void WriteHintFile(String hintFilePath) throws IOException {

    }


    public void append(long stationID, String message) {
        long appendingTimestamp = System.currentTimeMillis();
        int recordLength = 18 + message.length();
        try {
            // check if it's the first segment or
            // the segment reached it's maximum size to create the next segment
            File file = new File( BITCASK_LOG_PATH + currentLogName + FILES_EXTENSION);
            if (numberOfCurrentLogs == 0 || (file.exists() && file.length() + recordLength > MAX_LOG_FILE_SIZE)) {
                // write hint file for the current hashmap to the disk
                if (numberOfCurrentLogs != 0)
                    WriteHintFile(BITCASK_LOG_PATH + HINT_FILE_PREFIX + currentLogName + FILES_EXTENSION);
                // change the current log file name
                currentLogName = LOG_FILE_PREFIX + appendingTimestamp;
                numberOfCurrentLogs++;
                if (numberOfCurrentLogs > MAX_LOG_FILE_COUNT)
                    runCompaction();
            }

            // append to the current segment
            FileOutputStream fos = new FileOutputStream(BITCASK_LOG_PATH + currentLogName + FILES_EXTENSION, true);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeLong(stationID); // 8 bytes
            dos.writeLong(appendingTimestamp); // 8 bytes timestamp of appending
            dos.writeShort(message.length()); // 2 bytes
            dos.writeBytes(message); // message.length() bytes
            dos.close();

            // update the in-memory hashmap
            long fileID = Long.parseLong(currentLogName.substring(currentLogName.indexOf("_") + 1));
            inMemoryHashmap.put(stationID, new RecentLocation(appendingTimestamp, fileID, (short) message.length(), currentByte + 18 ));
            currentByte += recordLength;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runCompaction() {
        // 
        File folder = new File(BITCASK_LOG_PATH);
        File[] files = folder.listFiles();
        if (files == null) {
            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\"");
        }
        List<String> filesNames = new ArrayList<>();
        for (File file : files) {
            if (file.isFile() && file.getName().startsWith(HINT_FILE_PREFIX))
                filesNames.add(file.getName());
        }

        //
        Map<Long, RecentLocation> compactedHintFile = new HashMap<>();






    }


}
