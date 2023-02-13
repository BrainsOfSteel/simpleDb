package com.simple.database.database.replica;
import com.simple.database.database.StateReloader;
import java.io.*;

public class LogAwareWriteAheadReader {

    private String walFileName;
    private static LogAwareWriteAheadReader logAwareWriteAheadReader;
    private FileWriter fileWriter;

    private LogAwareWriteAheadReader(String walFileName) {
        try {
            this.walFileName = walFileName;
            this.fileWriter = new FileWriter(walFileName, true);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to create replica read file");
            System.exit(0);
        }
    }

    public static LogAwareWriteAheadReader getInstance(String fileName) {
        if(logAwareWriteAheadReader == null){
            logAwareWriteAheadReader  = new LogAwareWriteAheadReader(fileName);
        }
        return logAwareWriteAheadReader;
    }


    public void reloadState(StateReloader stateReloader) {
        try(BufferedReader reader = new BufferedReader(new FileReader(walFileName))){
            String line = null;
            while((line = reader.readLine()) != null){
                stateReloader.reloadState(line);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Unable to reloadReplica State");
            System.exit(0);
        }
    }

}
