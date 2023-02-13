package com.simple.database.database.service;

import com.simple.database.database.replica.engine.DatabaseEngine;
import org.springframework.stereotype.Service;

@Service
public class ReplicaDatabaseService {
    private DatabaseEngine databaseEngine = DatabaseEngine.getInstance();

    public String getValue(String key){
        return databaseEngine.getValue(key);
    }
}
