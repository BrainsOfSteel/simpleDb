package com.simple.database.database;

import java.util.List;

public interface StateReloader {
    public void reloadState(String logLine);

    default void reloadBulkState(List<String> logLines){
        for(String logLine : logLines){
            reloadState(logLine);
        }
    }
}
