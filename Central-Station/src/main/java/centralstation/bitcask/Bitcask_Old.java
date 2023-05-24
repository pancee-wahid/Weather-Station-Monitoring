package centralstation.bitcask;

import com.fasterxml.jackson.databind.ObjectMapper;
import message.Message;

import java.io.*;
import java.util.*;

public class Bitcask_Old {
    private static final String LOG_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\Central-Station\\bitcask\\logs";
    private static final String HINT_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\Central-Station\\bitcask\\hint_files";
    private static final String HINT_FILE_PREFIX = "hint_";
    private static final String LOG_FILE_PREFIX = "log_";
    private static final String FILES_EXTENSION = ".bin";
    private static final int MAX_LOG_FILE_SIZE = 1000; // 1 KB
    private static final int MAX_LOG_FILE_COUNT = 5; // maximum number of log files to keep before starting compaction
    private static final int NUM_OF_STATIONS = 10;
    private String currentLogName; // ToDo : what is the name of the first log file?
    private long currentByte;
    private int numberOfCurrentLogs;

    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * String: file_name -> Map<Long, Long>: hashmap of log file with name file_name
     * Long: station_id -> Long: byte_offset in the log file
     */
    private Map<String, Map<Long, Long>> inMemoryHashmaps;

    public Bitcask_Old() throws IOException {
        inMemoryHashmaps = new HashMap<>();
        currentByte = 0;
        numberOfCurrentLogs = 0;
        currentLogName = "";
        runCompaction();
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
     *
     * @param segmentFileName name of the hint file (without extension) to read and construct hashmap from
     * @return the reconstructed hashmap
     */
    private Map<Long, Long> readHintFile(String segmentFileName, boolean addToMemory) throws IOException {
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
        if (addToMemory)
            inMemoryHashmaps.put(segmentFileName, hashmap);
        return hashmap;
    }

    /**
     * append new key-value pair to the current segment
     *
     * @param key   the station id
     * @param value the status message sent by the station
     */
    public void append(long key, String value) throws IOException {
        try {
            // check if it's the first segment or
            // the segment reached it's maximum size to create the next segment
            int recordLength = 10 + value.length();
            File file = new File(LOG_FILES_PATH + "\\" + currentLogName + FILES_EXTENSION);
            if (numberOfCurrentLogs == 0 || (file.exists() && file.length() + recordLength > MAX_LOG_FILE_SIZE)) {
                // write the hint file (hashmap) of the old segment to the disk
                if (numberOfCurrentLogs != 0)
                    writeHintFile(currentLogName, inMemoryHashmaps.get(currentLogName));
                // change the current log file name
                Message msg = objectMapper.readValue(value, Message.class);
                currentLogName = LOG_FILE_PREFIX + msg.status_timestamp;
                numberOfCurrentLogs++;
            }

            // append to the current segment
            FileOutputStream fos = new FileOutputStream(LOG_FILES_PATH + "\\" + currentLogName + FILES_EXTENSION, true);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeLong(key); // 8 bytes
            dos.writeLong(System.currentTimeMillis()); // 8 bytes timestamp of appending
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

    private void writeHintFile(String logFileName, Map<Long, Long> hashmap) throws IOException {
        try {
            // write the hint file of the log file with the specified name
            FileOutputStream fos = new FileOutputStream(HINT_FILES_PATH + "\\" + HINT_FILE_PREFIX + logFileName + FILES_EXTENSION, true);
            DataOutputStream dos = new DataOutputStream(fos);
            for (Map.Entry<Long, Long> hashmapEntry : hashmap.entrySet()) {
                dos.writeLong(hashmapEntry.getKey()); // 8 bytes - station_id
                dos.writeLong();
                dos.writeLong(hashmapEntry.getValue()); // 8 bytes - byte_offset
            }
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // ToDo
    public void runCompaction() throws IOException {
        Thread thread = new Thread(() -> {
            while (true) {
                // -> wait until numberOfCurrentLogs exceeds MAX_LOG_FILE_COUNT <-
                while (numberOfCurrentLogs <= MAX_LOG_FILE_COUNT) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                // -> perform compaction <-
                // 1. sort the log files by their modification time and remove the currentLog being appended to
                File folder = new File(LOG_FILES_PATH);
                File[] files = folder.listFiles();
                if (files == null) {
                    System.out.println("No log files in \"" + LOG_FILES_PATH + "\"");
                    break;
                }
                List<String> filesNames = new ArrayList<>();
                for (File file : files) {
                    if (file.isFile())
                        filesNames.add(file.getName());
                }
                filesNames.remove(currentLogName + FILES_EXTENSION); // exclude the current segment being updated
                Collections.sort(filesNames);

                // 2. merge the log files into a new compacted log file
                String compactedFileName = "compacted_" + filesNames.get(0);
                try {
                    FileInputStream fis;
                    FileOutputStream fos = new FileOutputStream(LOG_FILES_PATH + "\\" + compactedFileName, true);
                    File file;
                    byte[] readBytes;
                    for (String fileName: filesNames) {
                        // read the contents of the segment to be merged
                        file = new File(LOG_FILES_PATH + "\\" + fileName);
                        fis = new FileInputStream(LOG_FILES_PATH + "\\" + fileName);
                        readBytes = new byte[(int) file.length()];
                        fis.read(readBytes);
                        fis.close();

                        // append the contents of the file to the compacted file
                        fos.write(readBytes);
                    }
                    fos.close();
                } catch (IOException e) {
                    System.out.println("Error in compaction to \"" + compactedFileName + "\".");
                }

                // 3. construct the hashmap of the compacted log file
                Map<Long, Long> newHashmap = new HashMap<>();
                for (int i = filesNames.size() - 1; i >= 0 ; i--) {
                    String name = filesNames.get(i).substring(0, filesNames.get(i).indexOf(FILES_EXTENSION));
                    try {
                        Map<Long, Long> oldHashmap = readHintFile(name, false);
                        for (Map.Entry<Long, Long> entry : oldHashmap.entrySet()) {
                            if (!newHashmap.containsKey(entry.getKey()))
                                newHashmap.put(entry.getKey(), entry.getValue());
                        }
                    } catch (IOException e) {
                        System.out.println("Error on reading hint file \"" + name + "\".");
                    }
                }

                // 4. delete the old logfiles and hint files
                inMemoryHashmaps.put(filesNames.get(0).substring(0, filesNames.get(0).indexOf(FILES_EXTENSION)), newHashmap);
                File file;
                for (String fileName: filesNames) {
                    file = new File(HINT_FILES_PATH + "\\" + HINT_FILE_PREFIX + fileName);
                    while (!file.delete()) {
                        System.out.println("Failed deleting \"hint_" + fileName + "\"");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    file = new File(LOG_FILES_PATH + "\\" + fileName);
                    while (!file.delete()) {
                        System.out.println("Failed deleting \"" + fileName + "\"");
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    numberOfCurrentLogs--;
                    inMemoryHashmaps.remove(filesNames.get(0).substring(0, filesNames.get(0).indexOf(FILES_EXTENSION)));
                }

                // 5. write and rename hint and log file after compaction
                String hintFileName = filesNames.get(0).substring(0, filesNames.get(0).indexOf(FILES_EXTENSION));
                try {
                    writeHintFile(hintFileName, newHashmap);
                } catch (IOException e) {
                    System.out.println("Failed writing the new hint file");
                }

                file = new File(LOG_FILES_PATH + "\\" + compactedFileName);
                file.renameTo(new File(LOG_FILES_PATH + "\\" + filesNames.get(0)));
            }

        });
        thread.start();
    }

}