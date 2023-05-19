package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StationsRunner {
    public static void main(String[] args) throws IOException {
        String pidLogPath = "D:\\Projects\\Weather-Station-Monitoring\\Weather-Stations\\logs\\pid";

        // starting the 10 weather stations
        // add the 10 commands to be run
        List<String> commands = new ArrayList<>();
        for (int i = 1; i <= 10; i++)
            commands.add("java -jar weather-station.jar " + i);

        // delete the previous log file if it exists
        File file = new File(pidLogPath);
        if (file.exists()) {
            if (file.delete()) {
                System.out.println("The previous pid log file deleted successfully.");
            } else {
                System.out.println("Failed to delete the previous log file.");
            }
        } else {
            System.out.println("No previous log file.");
        }

        // start the processes
        List<Process> processes = new ArrayList<>();

        for (String command : commands) {
            // start the station
            ProcessBuilder pb = new ProcessBuilder(command.split(" "));
            pb.directory(new File("D:\\Projects\\Weather-Station-Monitoring\\Weather-Stations"));
            Process process = pb.start();
            processes.add(process);

            // Get the PID field of the process object
            long pid = process.pid();

            // write the PID to the log file
            try {
                FileWriter fileWriter = new FileWriter(pidLogPath, true);
                fileWriter.write(pid + "\n");
                fileWriter.close();
            } catch (IOException e) {
                System.out.println("An error occurred on writing to pid log file.");
                e.printStackTrace();
            }
        }

        for (Process process : processes) {
            Thread thread = new Thread(() -> {
                try {
                    process.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            thread.start();
        }

        System.out.println("All weather stations started.");
    }
}