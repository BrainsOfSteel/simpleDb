package com.simple.database.database.service;

import org.springframework.stereotype.Service;

import com.simple.database.database.master.engine.DatabaseEngine;

@Service
public class DatabaseService {
    private DatabaseEngine databaseEngine = DatabaseEngine.getInstance();

    public void addKey(String key, String value) throws Exception{
        databaseEngine.addKey(key, value);
    }

    public void delKey(String key) throws Exception{
        databaseEngine.delKey(key);
    }

    public String getValue(String key){
        return databaseEngine.getValue(key);
    }
}
