package com.simple.database.database.master.engine;

import java.util.concurrent.ConcurrentHashMap;

import com.simple.database.database.master.ReplicaAwareWriteAheadLog;
import com.simple.database.database.StateReloader;
import com.simple.database.database.utils.Util;

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
        return op+Util.KEY_VALUE_DELIMITER+ key +Util.KEY_VALUE_DELIMITER + value  + Util.KEY_VALUE_DELIMITER + Util.CHECKSUM_CHARACTER;
    }

    private String getDelKey(String op, String key){
        return op + Util.KEY_VALUE_DELIMITER + key + Util.KEY_VALUE_DELIMITER + Util.CHECKSUM_CHARACTER;
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
}
