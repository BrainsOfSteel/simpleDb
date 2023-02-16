package com.simple.database.database.master;

import com.simple.database.database.StateReloader;
import com.simple.database.database.utils.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicaAwareWriteAheadLog {
    private String writeAheadFileName;
    private static ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog;
    private FileWriter fileWriter;
    private ConcurrentHashMap<String, String> replicaHostVsFileName = new ConcurrentHashMap<>();
    private final int maxBatchSize = 10;
    private AtomicLong countRunningReplicaThreads = new AtomicLong(0);
    private AtomicBoolean cleanupSignal = new AtomicBoolean(false);
    private ExecutorService executorService;

    //Todo: Added to make sure that it does not interfere with the cleanup of WAL
    public void addReplica(String replicaHost, String fileName){
        if(replicaHostVsFileName.containsKey(replicaHost)){
            return;
        }
        else{
            replicaHostVsFileName.put(replicaHost, fileName);
            startReplicaTask(replicaHost, fileName);
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
            fileWriter.write(key);
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

    //Only to be called from DatabaseEngine cleanup
    public void stopReplicaThreadsAndCleanup() throws Exception {
        cleanupSignal.getAndSet(true);
        long count = -1L;
        while(countRunningReplicaThreads.get() > 0){
            count++;
            if(count % 200 == 0){
                System.out.println("Stopping replica threads");
            }
        }
        try {
            fileWriter.flush();
            fileWriter.close();
            Files.deleteIfExists(Path.of(writeAheadFileName));
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("Stopped replica Threads");
    }

    //Only to be called from DatabaseEngine cleanup
    public void restartReplicaThreads() throws Exception {
        //Reset all the parameters
        try {
            fileWriter = new FileWriter(writeAheadFileName,true);
            cleanupSignal.getAndSet(false);
            countRunningReplicaThreads.getAndSet(0L);
            for (Map.Entry<String, String> entry : replicaHostVsFileName.entrySet()) {
                startReplicaTask(entry.getKey(), entry.getValue());
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception("Unable to clean restart the write ahead log writer will abort");
        }
        System.out.println("Restarted the replica tasks");
    }

    private void startReplicaTask(String replicaHost, String fileName){
        SyncReplicasRunnable syncReplicasRunnable = new SyncReplicasRunnable(fileName, writeAheadFileName, replicaHost, maxBatchSize, countRunningReplicaThreads, cleanupSignal);
        executorService.submit(syncReplicasRunnable);
        countRunningReplicaThreads.incrementAndGet();
    }

    public String getWriteAheadFileName() {
        return writeAheadFileName;
    }
}