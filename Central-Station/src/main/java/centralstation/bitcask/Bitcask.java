package centralstation.bitcask;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;

public class Bitcask {
    private final String HINT_FILE_PREFIX = "hint_";
    private final String LOG_FILE_PREFIX = "log_";
    private final String FILES_EXTENSION = ".bin";
    private int MAX_LOG_FILE_SIZE;
    private int MAX_LOG_FILE_COUNT;
    private int NUM_OF_STATIONS;
    private String BITCASK_LOG_PATH;
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

    private void readHintFile(String hintFilePath, Map<Long, RecentLocation> map) throws IOException {
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
                if (!(map.containsKey(stationID) && map.get(stationID).timestamp > timestamp))
                    map.put(stationID, new RecentLocation(timestamp, fileID, valueSize, valueOffset));
            }
            dis.close();
        }
    }

    private void writeHintFile(String hintFilePath, Map<Long, RecentLocation> map) throws IOException {
        FileOutputStream fos = new FileOutputStream(hintFilePath, true);
        DataOutputStream dos = new DataOutputStream(fos);
        for (Map.Entry<Long, RecentLocation> entry : map.entrySet()) {
            dos.writeLong(entry.getKey()); // 8 bytes
            dos.writeLong(entry.getValue().timestamp); // 8 bytes
            dos.writeLong(entry.getValue().fileID); // 8 bytes
            dos.writeShort(entry.getValue().valueSize); // 2 bytes
            dos.writeLong(entry.getValue().valueOffset); // message.length() bytes
        }
        dos.close();
    }

    public void append(long stationID, String message) {
        long appendingTimestamp = System.currentTimeMillis();
        int recordLength = 18 + message.length();
        try {
            // check if it's the first segment or
            // the segment reached it's maximum size to create the next segment
            File file = new File(BITCASK_LOG_PATH + currentLogName + FILES_EXTENSION);
            if (numberOfCurrentLogs == 0 || (file.exists() && file.length() + recordLength > MAX_LOG_FILE_SIZE)) {
                // write hint file for the current hashmap to the disk
                if (numberOfCurrentLogs != 0)
                    writeHintFile(BITCASK_LOG_PATH + HINT_FILE_PREFIX + currentLogName + FILES_EXTENSION, inMemoryHashmap);
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
            synchronized (inMemoryHashmap) {
                inMemoryHashmap.put(stationID, new RecentLocation(appendingTimestamp, fileID, (short) message.length(), currentByte + 18));
            }
            currentByte += recordLength;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runCompaction() throws IOException {
        // get hint files of old segments
        List<String> hintFilesNames = getHintFilesNames();

        // get the recent locations of all keys using the saved hint files
        Map<Long, RecentLocation> compactedHintFile = new HashMap<>();
        for (int i = hintFilesNames.size() - 1; i >= 0; i--)
            readHintFile(BITCASK_LOG_PATH + hintFilesNames.get(i), compactedHintFile);

        // write the merged segment and the new hint file
        // and update current inMemoryHashmap if needed
        String mergedFilePath = BITCASK_LOG_PATH + "merged_" + hintFilesNames.get(0).replace(HINT_FILE_PREFIX, "");
        String mergedHintFilePath = BITCASK_LOG_PATH + "merged_" + hintFilesNames.get(0);
        merge(mergedFilePath, mergedHintFilePath, compactedHintFile);


//        for (Map.Entry<Long, RecentLocation> entry : compactedHintFile.entrySet()) {
//            long id = entry.getKey();
//            RecentLocation rc = entry.getValue();
//            synchronized (inMemoryHashmap) {
//                if (inMemoryHashmap.containsKey(id) && inMemoryHashmap.get(id).timestamp <= rc.timestamp)
//                    inMemoryHashmap.put(id, rc);
//            }
//        }

        // delete old segments and hint files
        File folder = new File(BITCASK_LOG_PATH);
        File[] files = folder.listFiles();
        if (files == null) {
            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\"");
            return;
        }
        for (File file : files) {
            if (file.isFile() && (!file.getName().startsWith("merged") && !file.getName().contains(currentLogName)))
                file.delete();
        }
        File file = new File(mergedFilePath);
        file.renameTo(new File(mergedFilePath.replace("merged_", "")));
        file = new File(mergedHintFilePath);
        file.renameTo(new File(mergedHintFilePath.replace("merged_", "")));
        numberOfCurrentLogs -= MAX_LOG_FILE_COUNT;
    }

    private List<String> getHintFilesNames() {
        File folder = new File(BITCASK_LOG_PATH);
        File[] files = folder.listFiles();
        if (files == null)
            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\"");
        List<String> hintFilesNames = new ArrayList<>();
        for (File file : files) {
            if (file.isFile() && file.getName().contains(HINT_FILE_PREFIX))
                hintFilesNames.add(file.getName());
        }
        Collections.sort(hintFilesNames);
        return hintFilesNames;
    }

    private void merge(String mergedFilePath, String mergedHintFilePath, Map<Long, RecentLocation> compactedHintFile) throws IOException {
        RandomAccessFile raf;
        long currentByte = 0;
        System.out.println("mergedFilePath:\n" + mergedFilePath);
        String fileID = mergedFilePath.substring(mergedFilePath.indexOf(LOG_FILE_PREFIX) + LOG_FILE_PREFIX.length(), mergedFilePath.indexOf(FILES_EXTENSION));
        long stationID, timestamp, valueOffset, newValueOffset;
        short valueSize;

        FileOutputStream fosSegment = new FileOutputStream(mergedFilePath, true);
        DataOutputStream dosSegment = new DataOutputStream(fosSegment);

        FileOutputStream fosHint = new FileOutputStream(mergedHintFilePath, true);
        DataOutputStream dosHint = new DataOutputStream(fosHint);

        for (Map.Entry<Long, RecentLocation> entry : compactedHintFile.entrySet()) {
            stationID = entry.getKey();
            timestamp = entry.getValue().timestamp;
            valueSize = entry.getValue().valueSize;
            valueOffset = entry.getValue().valueOffset;

            String filename = BITCASK_LOG_PATH + LOG_FILE_PREFIX + entry.getValue().fileID + FILES_EXTENSION;  // Replace with the name of your file
            try {
                // read from target file
                raf = new RandomAccessFile(filename, "r");
                raf.seek(valueOffset);
                byte[] message = new byte[valueSize];
                raf.read(message);
                raf.close();
                // write to merged file
                dosSegment.writeLong(stationID); // station_id 8
                dosSegment.writeLong(timestamp); // timestamp 8
                dosSegment.writeShort(valueSize); // value_size 2
                dosSegment.write(message); // message massage.length
                // write to merged hint file
                dosHint.writeLong(stationID); // station
                dosHint.writeLong(timestamp); // timestamp
                dosHint.writeLong(Long.parseLong(fileID)); // file_id
                dosHint.writeShort(valueSize); // value size
                newValueOffset = currentByte + 18;
                dosHint.writeLong(newValueOffset); // value offset
                currentByte += 18 + message.length;
                // update hashmap if needed
                synchronized (inMemoryHashmap) {
                    if (inMemoryHashmap.containsKey(stationID) && inMemoryHashmap.get(stationID).timestamp <= timestamp)
                        inMemoryHashmap.put(stationID, new RecentLocation(timestamp, Long.parseLong(fileID), valueSize, newValueOffset));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        dosSegment.close();
        dosHint.close();
    }

}
