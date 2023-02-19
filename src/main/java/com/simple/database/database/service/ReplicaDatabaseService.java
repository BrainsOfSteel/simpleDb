package com.simple.database.database.service;

import com.simple.database.database.replica.engine.DatabaseEngine;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ReplicaDatabaseService {
    private DatabaseEngine databaseEngine = DatabaseEngine.getInstance();

    public String getValue(String key){
        return databaseEngine.getValue(key);
    }

    public void addKey(long versionNumber, List<String> walLines, long sequenceNumber, int endLineNumber){
        databaseEngine.walLogsFromMaster(versionNumber, walLines, sequenceNumber, endLineNumber);
    }
}
