package centralstation.bitcask;

import java.io.*;

public class Test {

    private static final String LOG_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\CentralStation\\bitcask\\logs";
    private static final String HINT_FILES_PATH = "D:\\Projects\\Weather-Station-Monitoring\\CentralStation\\bitcask\\hint-files";
    private static final String HINT_FILE_PREFIX = "hint_";
    private static final String LOG_FILE_PREFIX = "log_";
    private static final String FILES_EXTENSION = ".bin";
    private static final int MAX_LOG_FILE_SIZE = 1000000; // 1 MB
    private static final int MAX_LOG_FILE_COUNT = 10; // maximum number of log files to keep before starting compaction
    private static final int NUM_OF_STATIONS = 10;
    private String currentLogName; // ToDo : what is the name of the first log file?
    private static long currentByte = 0;

    public static void main(String[] args) throws InterruptedException {
        Thread thread = new Thread(() -> {
//            int g = 100;
//            while (g > 0) {
//                // Access instance variable x
//                System.out.println(currentByte);
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                g--;
//            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            currentByte = 742;
        });
        thread.start();


        for (int i = 0; i < 10; i++) {
            currentByte += 5;Thread.sleep(10);

        }
        Thread.sleep(10000);

        System.out.println(currentByte);
    }
}

/*
        try() {
            String path = "D:\\Projects\\Weather-Station-Monitoring\\CentralStation\\bitcask\\test.bin";
            FileOutputStream fos = new FileOutputStream(path, true);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeLong(1); // 8 bytes
            String value = "{\"station_id\":1,\"s_no\":1,\"battery_status\":\"low\"," +
                    "\"status_timestamp\":1681521224,\"weather\":{\"humidity\":35," +
                    "\"temperature\":100,\"wind_speed\":13}}";
            dos.writeShort(value.length()); // 2 bytes
            System.out.println(value.length() + 10);
            dos.writeBytes(value); // value.length() bytes
            dos.close();

            fos = new FileOutputStream(path, true);
            dos = new DataOutputStream(fos);
            dos.writeLong(5); // 8 bytes
            value = "{\"station_id\":5,\"s_no\":5,\"battery_status\":\"low\"," +
                    "\"status_timestamp\":168888824,\"weather\":{\"humidity\":80," +
                    "\"temperature\":150,\"wind_speed\":130}}";
            dos.writeShort(value.length()); // 2 bytes
            System.out.println(value.length() + 10);
            dos.writeBytes(value); // value.length() bytes
            dos.close();

            File file = new File(path);
            System.out.println(file.length());
            System.out.println("Written");
            System.out.println("Reading");

            try (FileInputStream fis = new FileInputStream(path)) {
                DataInputStream dis = new DataInputStream(fis);
                while (dis.available() > 0) {
                    long id = dis.readLong();
                    System.out.println(id);
                    short length = dis.readShort();
                    System.out.println(length);
                    byte[] bytes = new byte[length];
                    dis.readFully(bytes);
                    String text = new String(bytes);
                    System.out.println(text);
                }
                dis.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
 */