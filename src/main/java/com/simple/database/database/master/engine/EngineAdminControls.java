package com.simple.database.database.master.engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EngineAdminControls {

    @Autowired
    private SnapshotManager snapshotManager;

    private DatabaseEngine databaseEngine = DatabaseEngine.getInstance();

    //any other way to stop the data base will result
    public void cleanStopDatabase(){
        //acquire lock for database engine
        synchronized(databaseEngine){
            synchronized(snapshotManager){
                databaseEngine.stopDatabaseEngine();
                snapshotManager.stopSnapshots();
                System.out.println("Database shutdown completed");
                System.exit(0);
            }
        }
    }       
}
