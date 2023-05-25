package centralstation;

import centralstation.bitcask.RecentLocation;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Test {
    public static void main(String[] args) throws FileNotFoundException {
        String file = "/home/mohamed-yasser/Projects/Weather Station/Weather-Station-Monitoring/Central-Station/bitcask/log_1685052422111.bin";
        long stationID, timestamp;
        short valueSize;
        try (FileInputStream fis = new FileInputStream(file)) {
            DataInputStream dis = new DataInputStream(fis);
//            RandomAccessFile raf = new RandomAccessFile(file, "r");
//            raf.seek(8);
//            byte[] message = new byte[200];
//            raf.read(message);
//            raf.close();
//            System.out.println(new String(message, StandardCharsets.US_ASCII));

            while (dis.available() > 0) {
                stationID = dis.readLong();
                timestamp = dis.readLong();
                valueSize = dis.readShort();
                byte[] m = new byte[valueSize];
                dis.read(m);
                System.out.println(stationID + " " + timestamp + " " + valueSize + " " +  new String(m, StandardCharsets.UTF_8));
            }
            dis.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
