package centralstation;

import centralstation.bitcask.Bitcask;

public class CentralStation {
    private static final String BITCASK_LOG_PATH = "D:\\Projects\\Weather-Station-Monitoring\\Central-Station\\bitcask\\";
    private static final int MAX_LOG_FILE_SIZE = 1000; // 1 KB
    private static final int MAX_LOG_FILE_COUNT = 5; // maximum number of log files to keep before starting compaction
    private static final int NUM_OF_STATIONS = 10;

    public static void main(String[] args) {
        Bitcask bitcask = new Bitcask(BITCASK_LOG_PATH, MAX_LOG_FILE_SIZE, MAX_LOG_FILE_COUNT, NUM_OF_STATIONS);

    }
}
