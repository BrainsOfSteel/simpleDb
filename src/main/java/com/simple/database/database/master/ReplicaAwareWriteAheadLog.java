package com.simple.database.database.master;

import com.simple.database.database.StateReloader;
import com.simple.database.database.utils.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicaAwareWriteAheadLog {
    private String writeAheadFileName;
    private static ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog;
    private FileWriter fileWriter;
    private ConcurrentHashMap<String, String> replicaHostVsFileName = new ConcurrentHashMap<>();
    private final int maxBatchSize = 10;
    private ExecutorService executorService;

    public void addReplica(String replicaHost, String fileName){
        if(replicaHostVsFileName.containsKey(replicaHost)){
            return;
        }
        else{
            replicaHostVsFileName.put(replicaHost, fileName);
            SyncReplicasRunnable syncReplicasRunnable = new SyncReplicasRunnable(fileName, writeAheadFileName, replicaHost, maxBatchSize);
            executorService.submit(syncReplicasRunnable);
        }
    }

    private ReplicaAwareWriteAheadLog(String fileName) {
        try {
            this.writeAheadFileName = fileName;
            fileWriter = new FileWriter(fileName, true);
            executorService = Executors.newCachedThreadPool();
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