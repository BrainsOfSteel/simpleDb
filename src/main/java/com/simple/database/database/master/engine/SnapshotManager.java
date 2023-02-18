package com.simple.database.database.master.engine;

import com.simple.database.database.mode.DatabaseMode;
import com.simple.database.database.mode.ModeEnum;
import com.simple.database.database.utils.Util;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class SnapshotManager implements InitializingBean {
    private int maxSnapshotCount = 5;
    private String snapshotListFileName = "snapshotListFileName.log";
    private List<String> snapshotNames = new ArrayList<>();
    private String snapshotPrefix = "snapshot_";
    private ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(1);
    private DatabaseEngine databaseEngine = DatabaseEngine.getInstance();
    private AtomicBoolean hasSnapListNameFileCorrupted = new AtomicBoolean(false);

    @Override
    public void afterPropertiesSet() {
        DatabaseMode databaseMode = DatabaseMode.getInstance();
        if(databaseMode.getModeEnum() != ModeEnum.MASTER){
            System.out.println("Database snapshot disabled for replica");
            return;
        }

        System.out.println("Starting snapshot manager");
        if(!Files.exists(Path.of(snapshotListFileName))){
            try(FileWriter fw = new FileWriter(snapshotListFileName)){}
            catch (Exception e){
                System.out.println("snapshot manager not cleanly started...new snapshots will not be created");
                e.printStackTrace();
                return;
            }
        }
        else{
            try(BufferedReader br = new BufferedReader(new FileReader(snapshotListFileName))){
                String line = null;
                while((line = br.readLine()) != null){
                    if(!line.contains(""+Util.CHECKSUM_CHARACTER)){
                        System.out.println("snapshot list corrupted");
                        break;
                    }
                    String [] fileNameLine = line.split("" + Util.CHECKSUM_CHARACTER);
                    snapshotNames.add(fileNameLine[0]);
                }
            }catch (Exception e){
                System.out.println("snapshot manager not cleanly started...new snapshots will not be created");
                e.printStackTrace();
                return;
            }
        }
        System.out.println("Starting snapshot manager scheduler");
        executorService.scheduleAtFixedRate(createSnapshotsAndPersist(), 0, 1, TimeUnit.HOURS);
    }

    public void stopSnapshots(){
        synchronized(this){
            ///stop the database engine
            executorService.shutdown();
            System.out.println("Snapshot manager shutdown completed");
            //
        }
    }

    private Runnable createSnapshotsAndPersist(){
        return () -> {
            synchronized(this){
                if(hasSnapListNameFileCorrupted.get()){
                    System.out.println("Snapshot list file has corrupted.....new snapshots will not created until fixed....fixing to be added soon");
                    return;
                }
                String fileName = snapshotPrefix + System.currentTimeMillis() +".log";
                try{
                    databaseEngine.createSnapshot(fileName);
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("Unable to create snapshot...Will not perform other operations");
                    return;
                }

                try {
                    if(snapshotNames.size() < maxSnapshotCount){
                        snapshotNames.add(fileName);
                    }
                    else{
                        String fileToDelete = snapshotNames.get(0);
                        snapshotNames.remove(0);
                        snapshotNames.add(fileName);
                        //If the snapshot is not deleted, then it will stay but will not be recorded in the snapshot list name
                        Files.deleteIfExists(Path.of(fileToDelete));
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("Unable to delete the previous snapshot file....but this will not be recorded as the file in snapshot versions");
                }
                try(FileWriter fileWriter = new FileWriter(snapshotListFileName)){
                    for(String fName : snapshotNames){
                        fileWriter.write(fName+Util.CHECKSUM_CHARACTER+Util.ENTRY_DELIMITER);
                    }
                    fileWriter.flush();
                }catch (Exception e){
                    System.out.println("inconsistent state of file manager.....manual intervention required to clean. Not a catastrophic failure");
                    hasSnapListNameFileCorrupted.getAndSet(true);
                    e.printStackTrace();
                }
            }
        };
    }
}
