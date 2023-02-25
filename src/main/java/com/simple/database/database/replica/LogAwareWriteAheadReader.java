package com.simple.database.database.replica;
import com.simple.database.database.StateReloader;
import com.simple.database.database.utils.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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

    public static LogAwareWriteAheadReader getTestInstance(String fileName) throws Exception {
        String testType = System.getenv("testType");
        if(Util.UNIT_TEST.equals(testType)) {
            logAwareWriteAheadReader = new LogAwareWriteAheadReader(fileName);
            return logAwareWriteAheadReader;
        }
        throw new Exception("Invalid operation");
    }

    public void closeFiles(){
        try {
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //add logic to survive the database crash
    public void reloadStateFromBlockCommitFile(StateReloader state){
        boolean isStartFound = false;
        try(BufferedReader reader = new BufferedReader(new FileReader(walFileName))){
            String line = null;
            List<String> collectedLines = new ArrayList<>();
            while((line = reader.readLine())!=null){
                if(Util.BLOCK_START.equals(line)){
                    if(isStartFound){
                        collectedLines.clear();
                    }
                    else {
                        isStartFound = true;
                    }
                }
                else if(Util.BLOCK_END.equals(line)){
                    if(isStartFound){
                        state.reloadBulkState(collectedLines);
                        isStartFound = false;
                        collectedLines.clear();
                    }
                    else{
                        collectedLines.clear();
                    }
                }
                else{
                    if(isStartFound){
                        collectedLines.add(line);
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("Unable to load replica state during boot");
            System.exit(0);
        }
    }

    public void reloadStateFromFile(StateReloader stateReloader) {
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

    public void appendToWal(List<String> walLines) {
        try {
            String blockString = generateBlock(walLines);
            fileWriter.write(blockString);
            fileWriter.flush();
        }catch (Exception e){
            e.printStackTrace();
            try {
                fileWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            System.out.println("error while appending to write ahead log file....aborting");
            System.exit(0);
        }
    }

    private String generateBlock(List<String> walLines) {
        StringBuilder sb = new StringBuilder("");
        sb.append(Util.BLOCK_START).append(Util.ENTRY_DELIMITER);
        for(String walLine: walLines){
            sb.append(walLine);
        }
        sb.append(Util.BLOCK_END).append(Util.ENTRY_DELIMITER);
        return sb.toString();
    }
}
