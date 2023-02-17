package com.simple.database.database.master.engine;

import com.simple.database.database.StateReloader;
import com.simple.database.database.master.ReplicaAwareWriteAheadLog;
import com.simple.database.database.utils.Util;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class DatabaseEngine implements StateReloader{

    private static DatabaseEngine databaseEngine;
    private ConcurrentHashMap<String, String> keyValuePair;
    private final ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog;

    private DatabaseEngine(ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog) {
        keyValuePair = new ConcurrentHashMap<>();
        this.replicaAwareWriteAheadLog = replicaAwareWriteAheadLog;
    }  
       
    public static DatabaseEngine getInstance(ReplicaAwareWriteAheadLog replicaAwareWriteAheadLog) {
        if(databaseEngine == null){
            databaseEngine  = new DatabaseEngine(replicaAwareWriteAheadLog);
        }

        replicaAwareWriteAheadLog.reloadStateFromWal(databaseEngine);
        return databaseEngine;
    }

    public synchronized void addReplica(String restEndPoint, String fileName){
        replicaAwareWriteAheadLog.addReplica(restEndPoint, fileName);
    }

    public void reloadState(String logLine){
        Util.reloadState(logLine, keyValuePair);
    }

    public static DatabaseEngine getInstance(){
        return databaseEngine;
    }

    public synchronized void addKey(String key, String val) throws Exception{
        if(key == null || val == null){
            throw new Exception("Invalid input provided");
        }

        String appendLogLine = getAddKey(Util.ADD_OPERATION, key,val);
        replicaAwareWriteAheadLog.appendToWal(appendLogLine);
        keyValuePair.put(key, val);        
    }

    private String getAddKey(String op, String key, String value){
        return replicaAwareWriteAheadLog.getVersionNumber() + Util.KEY_VALUE_DELIMITER + op+Util.KEY_VALUE_DELIMITER+ key
                +Util.KEY_VALUE_DELIMITER + value  + Util.KEY_VALUE_DELIMITER + Util.CHECKSUM_CHARACTER + Util.ENTRY_DELIMITER;
    }

    private String getDelKey(String op, String key){
        return replicaAwareWriteAheadLog.getVersionNumber() + Util.KEY_VALUE_DELIMITER + op + Util.KEY_VALUE_DELIMITER
                + key + Util.KEY_VALUE_DELIMITER + Util.CHECKSUM_CHARACTER + Util.ENTRY_DELIMITER;
    }

    public synchronized void cleanupWriteAheadLog() throws Exception{
        String walTempFile = "walMasterTemp.log";
        try(FileWriter fw = new FileWriter(walTempFile)) {
            replicaAwareWriteAheadLog.incrementVersionNumber();
            for(Map.Entry<String, String> entry : keyValuePair.entrySet()){
                String appendLogLine = getAddKey(Util.ADD_OPERATION, entry.getKey(), entry.getValue());
                fw.write(appendLogLine);
                fw.flush();
            }
            replicaAwareWriteAheadLog.stopReplicaThreadsAndCleanup();
            Files.move(Path.of(walTempFile), Path.of(replicaAwareWriteAheadLog.getWriteAheadFileName()), ATOMIC_MOVE);
            replicaAwareWriteAheadLog.restartReplicaThreads();
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("Something bad has happened, irrecoverable failure, manually clean up the walLogs and restart the application or use snapshotted database to work");
            System.exit(0);
        }
    }

    public synchronized void delKey(String key){
        if(!keyValuePair.containsKey(key)){
            return;
        }

        String appendLogKey = getDelKey(Util.DEL_OPERATION, key);
        replicaAwareWriteAheadLog.appendToWal(appendLogKey);
        keyValuePair.remove(key);
    }

    public String getValue(String key){
        return keyValuePair.get(key);        
    }

    public synchronized void createSnapshot(String fileName) throws Exception{
        try(FileWriter fw = new FileWriter(fileName)){
            for(Map.Entry<String, String> entry: keyValuePair.entrySet()){
                fw.write(getAddKey(Util.ADD_OPERATION, entry.getKey(), entry.getValue()));
            }
            fw.flush();
        }catch (Exception e){
            throw e;
        }
    }
}
