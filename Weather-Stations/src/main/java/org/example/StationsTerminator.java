package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StationsTerminator {

    public static void main(String[] args) {
        // read processes ids from the log file
        String pidLogPath = "D:\\Projects\\Weather-Station-Monitoring\\Weather-Stations\\logs\\pid";
        List<String> pid = new ArrayList<>();
        try {
            // read the initial graph file
            BufferedReader reader = new BufferedReader(new FileReader(pidLogPath));
            String line;
            while ((line = reader.readLine()) != null)
                pid.add(line);
            reader.close();
        } catch (Exception e) {
            System.err.println("Exception: " + e);
            e.printStackTrace();
        }

        for (String i : pid) {
            try {
                ProcessBuilder pb = new ProcessBuilder("taskkill", "/F", "/PID", i);
                Process process = pb.start();
                int exitCode = process.waitFor();
                if (exitCode == 0) {
                    System.out.println("Process " + i + " successfully terminated.");
                } else {
                    System.out.println("Failed to terminate process" + i + ".");
                }
            } catch (IOException e) {
                System.err.println("Error starting process " + i + ": " + e.getMessage());
            } catch (InterruptedException e) {
                System.err.println("Process " + i + " execution interrupted: " + e.getMessage());
            }
        }
    }
}

