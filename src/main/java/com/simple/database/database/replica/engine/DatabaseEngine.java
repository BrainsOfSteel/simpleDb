package com.simple.database.database.replica.engine;

import com.simple.database.database.StateReloader;
import com.simple.database.database.replica.LogAwareWriteAheadReader;
import com.simple.database.database.utils.Util;

import java.util.concurrent.ConcurrentHashMap;

public class DatabaseEngine implements StateReloader {
    private static DatabaseEngine databaseEngine;
    private ConcurrentHashMap<String, String> keyValuePair;
    private final LogAwareWriteAheadReader logAwareWriteAheadReader;

    private DatabaseEngine(LogAwareWriteAheadReader logAwareWriteAheadReader){
        this.logAwareWriteAheadReader = logAwareWriteAheadReader;
        keyValuePair = new ConcurrentHashMap<>();
    }
    public static DatabaseEngine getInstance(LogAwareWriteAheadReader logAwareWriteAheadReader){
        if(databaseEngine == null){
            databaseEngine = new DatabaseEngine(logAwareWriteAheadReader);
        }
        logAwareWriteAheadReader.reloadState(databaseEngine);
        return databaseEngine;
    }

    public static DatabaseEngine getInstance(){
        return databaseEngine;
    }

    @Override
    public void reloadState(String logLine) {
        Util.reloadState(logLine, keyValuePair);
    }

    public String getValue(String key){
        return keyValuePair.get(key);
    }
}
