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
        //acquire lock for database engine and snapshot manager
        //keep this order same otherwise deadlock -> lock(snapshot manager) -> lock(databaseEngine)
        synchronized(snapshotManager){
            synchronized(databaseEngine){
                databaseEngine.stopDatabaseEngine();
                snapshotManager.stopSnapshots();
                System.out.println("Database shutdown completed");
                System.exit(0);
            }
        }
    }       
}
