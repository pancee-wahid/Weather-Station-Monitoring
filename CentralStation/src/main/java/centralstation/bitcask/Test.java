package centralstation.bitcask;

import java.io.*;

public class Test {

    public static void main(String[] args) {
        try {
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
    }
}

