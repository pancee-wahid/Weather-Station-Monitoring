package centralstation.bitcask;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private boolean recovered;

    public Bitcask(String BITCASK_LOG_PATH, int MAX_LOG_FILE_SIZE, int MAX_LOG_FILE_COUNT, int NUM_OF_STATIONS) throws IOException {
        this.BITCASK_LOG_PATH = BITCASK_LOG_PATH;
        this.MAX_LOG_FILE_SIZE = MAX_LOG_FILE_SIZE;
        this.MAX_LOG_FILE_COUNT = MAX_LOG_FILE_COUNT;
        this.NUM_OF_STATIONS = NUM_OF_STATIONS;
        inMemoryHashmap = new HashMap<>();
        currentByte = 0;
        numberOfCurrentLogs = 0;
        currentLogName = "";
        recovered = false;
        recover();
    }

//    private void recover() {
//        System.out.println("entered recover()");
//        File folder = new File(BITCASK_LOG_PATH);
//        File[] allFiles = folder.listFiles();
//        if (allFiles == null) {
//            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\"");
//            return;
//        }
//        List<String> hintFilesNames = new ArrayList<>();
//        for (File file : allFiles) {
//            if (file.isFile())
//                file.delete();
//        }
//    }

    private void recover() throws IOException {
        // get log and hint files in bitcask folder
        File folder = new File(BITCASK_LOG_PATH);
        File[] allFiles = folder.listFiles();
        if (allFiles == null) {
            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\" to recover from.\n Starting Normally.");
            return;
        }
        Pattern pattern = Pattern.compile("\\d+");
        List<Long> hintFiles = new ArrayList<>();
        List<Long> logFiles = new ArrayList<>();
        Matcher matcher;
        long id;
        for (File file : allFiles) {
            if (!file.isFile())
                continue;

            if (file.getName().contains("merged")) {
                file.renameTo(new File(file.getName().replace("merged_", "")));
            }

            matcher = pattern.matcher(file.getName());
            if (!matcher.find())
                continue;
            id = Long.parseLong(matcher.group());
            if (file.getName().contains(HINT_FILE_PREFIX))
                hintFiles.add(id);
            else if (file.getName().contains(LOG_FILE_PREFIX))
                logFiles.add(id);
        }

        // if no log or hint files, return
        if (logFiles.size() == 0 || hintFiles.size() == 0) {
            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\" to recover from.\n Starting Normally.");
            return;
        }

        // get the files that don't have a hint file
        List<Long> filesWithoutHint = new ArrayList<>();
        for (Long f : logFiles) {
            if (!hintFiles.contains(f))
                filesWithoutHint.add(f);
        }

        // construct a hashmap for the files that don't have hint files
        Map<Long, RecentLocation> compactedHintFile = new HashMap<>();
        for (Long f : filesWithoutHint)
            readLogFile(f, compactedHintFile);

        // merge these hint files
        // get the recent locations of all keys using the saved hint files
        String fileName;
        for (int i = hintFiles.size() - 1; i >= 0; i--) {
            fileName = BITCASK_LOG_PATH + HINT_FILE_PREFIX + LOG_FILE_PREFIX + hintFiles.get(i) + FILES_EXTENSION;
            readHintFile(fileName, compactedHintFile);
        }

        // generate the new merged file and the new hint file
        Collections.sort(logFiles);
        String mergedFilePath = BITCASK_LOG_PATH + "merged_" + LOG_FILE_PREFIX + logFiles.get(0) + FILES_EXTENSION;
        String mergedHintFilePath = BITCASK_LOG_PATH + "merged_" + HINT_FILE_PREFIX + LOG_FILE_PREFIX + logFiles.get(0) + FILES_EXTENSION;
        merge(mergedFilePath, mergedHintFilePath, compactedHintFile);

        // delete and rename
        deleteOldFiles();
        File file = new File(mergedFilePath);
        file.renameTo(new File(mergedFilePath.replace("merged_", "")));
        file = new File(mergedHintFilePath);
        file.renameTo(new File(mergedHintFilePath.replace("merged_", "")));
        recovered = true;
    }

    private void deleteOldFiles() {
        File folder = new File(BITCASK_LOG_PATH);
        File[] files = folder.listFiles();
        if (files == null) {
            System.out.println("No log files in \"" + BITCASK_LOG_PATH + "\" to delete.");
            return;
        }
        for (File file : files) {
            if (file.isFile() && (!file.getName().startsWith("merged") && !file.getName().contains(currentLogName)))
                file.delete();
        }
    }

    private void readLogFile(Long fileID, Map<Long, RecentLocation> map) {
        long stationID, timestamp;
        long valueOffset = 0;
        short valueSize;
        byte[] bytes;
        String filePath = BITCASK_LOG_PATH + LOG_FILE_PREFIX + fileID + FILES_EXTENSION;
        try (FileInputStream fis = new FileInputStream(filePath)) {
            DataInputStream dis = new DataInputStream(fis);
            while (dis.available() > 0) {
                stationID = dis.readLong();
                timestamp = dis.readLong();
                valueSize = dis.readShort();
                bytes = new byte[valueSize];
                dis.read(bytes);
                if (!(map.containsKey(stationID) && map.get(stationID).timestamp > timestamp))
                    map.put(stationID, new RecentLocation(timestamp, fileID, valueSize, valueOffset + 18));
            }
            dis.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            dos.writeLong(entry.getKey()); // 8 bytes - station_id
            dos.writeLong(entry.getValue().timestamp); // 8 bytes - timestamp
            dos.writeLong(entry.getValue().fileID); // 8 bytes - file_id
            dos.writeShort(entry.getValue().valueSize); // 2 bytes - value_size
            dos.writeLong(entry.getValue().valueOffset); // message.length() bytes - the message
        }
        dos.close();
    }

    public void append(long stationID, String message) {
        System.out.println("entered append()");
        long appendingTimestamp = System.currentTimeMillis();
        int recordLength = 18 + message.length();
        try {
            if (recovered || numberOfCurrentLogs == 0) {
                recovered = false;
                currentLogName = LOG_FILE_PREFIX + appendingTimestamp;
                File folder = new File(BITCASK_LOG_PATH);
                File[] allFiles = folder.listFiles();
                numberOfCurrentLogs = (allFiles == null) ? 0 : 1;
                currentByte = 0;
            } else {
                File file = new File(BITCASK_LOG_PATH + currentLogName + FILES_EXTENSION);
                if (file.exists() && (file.length() + recordLength > MAX_LOG_FILE_SIZE)) {
                    System.out.println("Basel");
                    writeHintFile(BITCASK_LOG_PATH + HINT_FILE_PREFIX + currentLogName + FILES_EXTENSION, inMemoryHashmap);
                    currentLogName = LOG_FILE_PREFIX + appendingTimestamp;
                    numberOfCurrentLogs++;
                    currentByte = 0;
                    if (numberOfCurrentLogs > MAX_LOG_FILE_COUNT)
                        runCompaction();
                }
            }

            // append to the current segment
            FileOutputStream fos = new FileOutputStream(BITCASK_LOG_PATH + currentLogName + FILES_EXTENSION, true);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeLong(stationID); // 8 bytes
            dos.writeLong(appendingTimestamp); // 8 bytes timestamp of appending
            dos.writeShort(message.length()); // 2 bytes
            dos.writeBytes(message); // message.length() bytes
            dos.close();

            System.out.println("data wrote to: " + BITCASK_LOG_PATH + currentLogName + FILES_EXTENSION);

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
        System.out.println("entered runCompaction()");
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

        // delete old segments and hint files
        deleteOldFiles();
        File file = new File(mergedFilePath);
        file.renameTo(new File(mergedFilePath.replace("merged_", "")));
        file = new File(mergedHintFilePath);
        file.renameTo(new File(mergedHintFilePath.replace("merged_", "")));
        numberOfCurrentLogs = numberOfCurrentLogs - MAX_LOG_FILE_COUNT + 1;
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
        FileInputStream fis;
        long currentByte = 0;
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

            String filename = BITCASK_LOG_PATH + LOG_FILE_PREFIX + entry.getValue().fileID + FILES_EXTENSION;
            try {
                // read from target file
                fis = new FileInputStream(filename);
                fis.skip(valueOffset);
                byte[] message = new byte[valueSize];
                fis.read(message);
                fis.close();

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
