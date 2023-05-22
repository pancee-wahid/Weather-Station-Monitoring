package centralstation.bitcask;

import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;

import java.io.*;
import java.util.*;

public class Bitcask {
    private static final String LOG_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\CentralStation\\bitcask\\logs";
    private static final String HINT_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\CentralStation\\bitcask\\hint-files";
    private static final String HINT_FILE_PREFIX = "hint_";
    private static final String LOG_FILE_PREFIX = "log_";
    private static final String FILES_EXTENSION = ".bin";
    private static final int MAX_LOG_FILE_SIZE = 1000000; // 1 MB
    private static final int MAX_LOG_FILE_COUNT = 10; // maximum number of log files to keep before starting compaction
    private static final int NUM_OF_STATIONS = 10;
    private String currentLogName; // ToDo : what is the name of the first log file?
    private long currentByte;

    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     *  String: file_name -> Map<Long, Long>: hashmap of log file with name file_name
     *  Long: station_id -> Long: byte_offset in the log file
     */
    private Map<String, Map<Long, Long>> inMemoryHashmaps;

    public Bitcask() {
        inMemoryHashmaps = new HashMap<>();
        currentByte = 0;
    }


    // ToDo - needs edit after changing concept of hint files
    private void recoverIfPossible() throws IOException {
        File folder = new File(HINT_FILES_PATH);
        File[] files = folder.listFiles();
        if (files == null) {
            System.out.println("No previous hint files in \"" + HINT_FILES_PATH + "\"");
            return;
        }

        System.out.println("Starting Recovery...");

        // get list of hint files names to read from the most recent
        List<String> filesNames = new ArrayList<>();
        for (File file : files) {
            if (file.isFile())
                filesNames.add(file.getName());
        }
        Collections.sort(filesNames);

        // use boolean array to check if all stations most recent messages were recovered
        boolean[] recovered = new boolean[NUM_OF_STATIONS];
        Arrays.fill(recovered, false);
        String currentFile;

        // read hint files from the latest one till reading the most recent message
        // of each station or the hint files are completely processed
        while (Arrays.asList(recovered).contains(false) && !filesNames.isEmpty()) {
            currentFile = filesNames.remove(filesNames.size() - 1);
//            readHintFile(currentFile, recovered);
            System.out.println("Used \"" + currentFile + "\"");
        }

        System.out.println("Recovery Completed.");
    }

    // ToDo

    /**
     * used in crash recovery
     * it adds the hashmap of the segment to the in-memory hashmaps
     * Note: hint file store station_id (Long) followed by byte_offset in the file (Long)
     * @param segmentFileName name of the hint file (without extension) to read and construct hashmap from
     * @return the reconstructed hashmap
     */
    private Map<Long, Long> readHintFile(String segmentFileName) throws IOException {
        Map<Long, Long> hashmap = new HashMap<>();
        String hintFileName = HINT_FILES_PATH + "\\" + HINT_FILE_PREFIX + segmentFileName + FILES_EXTENSION;
        long stationID, byteOffset;
        try (FileInputStream fis = new FileInputStream(hintFileName)) {
            DataInputStream dis = new DataInputStream(fis);
            while (dis.available() > 0) {
                stationID = dis.readLong();
                byteOffset = dis.readLong();
                hashmap.put(stationID, byteOffset);
            }
            dis.close();
        }
        inMemoryHashmaps.put(segmentFileName, hashmap);
        return hashmap;
    }

    /**
     * append new key-value pair to the current segment
     * @param key the station id
     * @param value the status message sent by the station
     */
    public void append(long key, String value) throws IOException {
        try {
            // check if the segment reached it's maximum size to create the next segment
            int recordLength = 10 + value.length();
            File file = new File(LOG_FILES_PATH + "\\" + currentLogName + FILES_EXTENSION);
            if (file.length() + recordLength > MAX_LOG_FILE_SIZE) {
                // write the hint file (hashmap) of the old segment to the disk
                writeHintFile(currentLogName);
                // change the current log file name
                Message msg = objectMapper.readValue(value, Message.class);
                currentLogName = LOG_FILE_PREFIX + msg.status_timestamp;
            }

            // append to the current segment
            FileOutputStream fos = new FileOutputStream(LOG_FILES_PATH + "\\" + currentLogName + FILES_EXTENSION, true);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeLong(key); // 8 bytes
            dos.writeShort(value.length()); // 2 bytes
            dos.writeBytes(value); // value.length() bytes
            dos.close();

            // update the in-memory hashmap of the current segment
            if (!inMemoryHashmaps.containsKey(currentLogName))
                inMemoryHashmaps.put(currentLogName, new HashMap<>());
            inMemoryHashmaps.get(currentLogName).put(key, currentByte);
            currentByte += recordLength;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeHintFile(String logFileName) throws IOException {
        try {
            // write the hint file of the log file with the specified name
            FileOutputStream fos = new FileOutputStream(HINT_FILES_PATH + "\\" + HINT_FILE_PREFIX + logFileName + FILES_EXTENSION, true);
            DataOutputStream dos = new DataOutputStream(fos);
            for (Map.Entry<Long, Long> hashmapEntry : inMemoryHashmaps.get(logFileName).entrySet()) {
                dos.writeLong(hashmapEntry.getKey()); // 8 bytes - station_id
                dos.writeLong(hashmapEntry.getValue()); // 8 bytes - byte_offset
            }
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // ToDo
    public void runCompaction(String[] logFiles) throws IOException {
        // sort the log files by their modification time
        // merge the log files into a new compacted log file
        // write a hint file for the compacted log file
        // delete the old logfiles and hint files
    }

}
