package com.simple.database.database.master;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;

import com.simple.database.database.StateReloader;
import com.simple.database.database.utils.Util;

public class ReplicaAwareWriteAheadLog {
    private String writeAheadFileName;
    private static ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog;
    private FileWriter fileWriter;
    private ConcurrentHashMap<String, Integer> replicaHostVsWALOffset = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> replicaHostVsFileName = new ConcurrentHashMap<>();
    private final int maxBatchSize = 10;

    public void addReplica(String replicaHost, String fileName){
        if(replicaHostVsWALOffset.containsKey(replicaHost)){
            return;
        }
        else{
            replicaHostVsFileName.put(replicaHost, fileName);
        }
    }
    private Runnable propogateLogsToReplicas(String replicaHost){
        return ()-> {
            String fileName = replicaHostVsFileName.get(replicaHost);
            int currentSeqNumber = 0;
            int endLineNumber = -1;
            try (FileWriter fw = new FileWriter(fileName, true)) {
                try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        String lineArr[] = line.split(Util.KEY_VALUE_DELIMITER);
                        if(!line.contains(Util.KEY_VALUE_DELIMITER)){
                            continue;
                        }
                        currentSeqNumber = Integer.parseInt(lineArr[0]);
                        endLineNumber = Integer.parseInt(lineArr[1]);
                    }
                }
                try (BufferedReader walReader = new BufferedReader(new FileReader(writeAheadFileName))) {
                    String line = null;
                    int count = 0;
                    while (count <= endLineNumber) {
                        count++;
                        walReader.readLine();
                    }

                    while (true) {
                        List<String> lines = new ArrayList<>();
                        int collectedLines = 0;
                        boolean flag = false;
                        while (collectedLines < maxBatchSize && (line = walReader.readLine()) != null) {
                            collectedLines++;
                            lines.add(line);
                            flag = true;
                        }
                        if(!flag){
                            continue;
                        }

                        currentSeqNumber++;
                        endLineNumber = endLineNumber < 0 ? collectedLines : endLineNumber  + collectedLines;
                        sendToTheReplicaHost(currentSeqNumber, endLineNumber, lines);
                        fw.write(currentSeqNumber + Util.KEY_VALUE_DELIMITER + endLineNumber + Util.ENTRY_DELIMITER);
                        fw.flush();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    private void sendToTheReplicaHost(int currentSeqNumber, int endNumber, List<String> lines) {
        System.out.println("seqNumber =" + currentSeqNumber + " endNumber = "+endNumber + " size =" + lines.size() + " lines = " + lines);
    }

    public static void main(String[] args) throws InterruptedException {
        ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog = ReplicaAwareWriteAheadLog.getInstance("walMaster.log");
        replicaAwareWriteAheadLog.addReplica("abc", "testFile1.txt");
        Thread t1 = new Thread(replicaAwareWriteAheadLog.propogateLogsToReplicas("abc"));
        t1.start();
        t1.join();
    }


    private ReplicaAwareWriteAheadLog(String fileName) {
        try {
            this.writeAheadFileName = fileName;
            fileWriter = new FileWriter(fileName, true);
        } catch (IOException e) {
            System.out.println("Unable to initialise the file writer.....exiting");
            e.printStackTrace();
            System.exit(0);
        }
    }  
       
    public static ReplicaAwareWriteAheadLog getInstance(String fileName) {      
        if(replicaAwareWriteAheadLog == null){
            replicaAwareWriteAheadLog  = new ReplicaAwareWriteAheadLog(fileName);
        }
        return replicaAwareWriteAheadLog;
    } 

    public void appendToWal(String key){
        try{
            fileWriter.write(key + Util.ENTRY_DELIMITER);
            fileWriter.flush();
        }catch(Exception e){
            System.out.println("Unable to append to WAL....exiting");
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void reloadStateFromWal(StateReloader stateReloader){
        try(BufferedReader br = new BufferedReader(new FileReader(writeAheadFileName))){
            String line = null;
            while((line = br.readLine())!=null){
                stateReloader.reloadState(line);
            }
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("Exiting application...unable to build state from log. wipe out and restart");
            System.exit(0);
        }
    }
}