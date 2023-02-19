package com.simple.database.database.replica.engine;

import com.simple.database.database.StateReloader;
import com.simple.database.database.replica.LogAwareWriteAheadReader;
import com.simple.database.database.utils.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseEngine implements StateReloader {
    private static DatabaseEngine databaseEngine;
    private ConcurrentHashMap<String, String> keyValuePair;
    private final LogAwareWriteAheadReader logAwareWriteAheadReader;
    private long latestVersionNumber; //Stateful to be persisted in the files
    private long currentSequenceNumber; // Retrive from the state
    private Map<Long, BufferedMessagesContainer> bufferedMessages = new TreeMap<>(); // Flush to the disk

    public Map<Long, BufferedMessagesContainer> getBufferedMessages() {
        return bufferedMessages;
    }

    public int getKeySetSize(){
        return keyValuePair.size();
    }

    private DatabaseEngine(LogAwareWriteAheadReader logAwareWriteAheadReader){
        this.logAwareWriteAheadReader = logAwareWriteAheadReader;
        keyValuePair = new ConcurrentHashMap<>();
    }
    public static DatabaseEngine getInstance(LogAwareWriteAheadReader logAwareWriteAheadReader){
        if(databaseEngine == null){
            databaseEngine = new DatabaseEngine(logAwareWriteAheadReader);
        }
        logAwareWriteAheadReader.reloadStateFromFile(databaseEngine);
        return databaseEngine;
    }

    public static DatabaseEngine getTestInstance(LogAwareWriteAheadReader logAwareWriteAheadReader) throws Exception {
        String testType = System.getenv("testType");
        if(Util.UNIT_TEST.equals(testType)) {
            databaseEngine = new DatabaseEngine(logAwareWriteAheadReader);
            return databaseEngine;
        }
        throw new Exception("Invalid operation");
    }

    private void syncBufferedMessagesToFile() {
    }

    public static DatabaseEngine getInstance(){
        return databaseEngine;
    }

    @Override
    public void reloadState(String logLine) {
        Util.loadStateFromLogLine(logLine, keyValuePair);
    }

    public String getValue(String key){
        return keyValuePair.get(key);
    }

    public synchronized void walLogsFromMaster(long versionNumber, List<String> walLines, long sequenceNumber, int endLineNumber) {
        if(versionNumber == latestVersionNumber){
            if(sequenceNumber == currentSequenceNumber){
                logAwareWriteAheadReader.appendToWal(walLines);
                addEntryToDatabase(walLines);
                currentSequenceNumber++;
                emptyValidBufferedMessages();
                syncToFile(versionNumber, currentSequenceNumber);
            }
            else if(sequenceNumber < currentSequenceNumber){
                System.out.println("Should not happen...something went wrong....skipping these messages "+ " version = "+versionNumber +" walLines = "+walLines
                        +"\n" + "sequenceNumber =" +sequenceNumber+" endLineNumber = "+endLineNumber);
            }
            else{
                //Buffer the messages
                bufferedMessages.put(sequenceNumber, new BufferedMessagesContainer(versionNumber, walLines, endLineNumber));
            }
        }
        else if(versionNumber > latestVersionNumber){
            //reset the bufferedMessages
            latestVersionNumber = versionNumber;
            bufferedMessages.clear();
            currentSequenceNumber = 0;
            walLogsFromMaster(versionNumber, walLines, sequenceNumber, endLineNumber);
        }
        else{
            System.out.println("arriving version number = "+versionNumber +" is less than current version number = "+latestVersionNumber
                    +"....skipping these messages .." + walLines);
        }
    }

    private void emptyValidBufferedMessages() {
        Set<Long> keysToRemove = new HashSet<>();
        for(Map.Entry<Long, BufferedMessagesContainer> entry: bufferedMessages.entrySet()){
            if(entry.getKey() == currentSequenceNumber){
                keysToRemove.add(entry.getKey());
                currentSequenceNumber++;
                logAwareWriteAheadReader.appendToWal(entry.getValue().getWalLines());
                addEntryToDatabase(entry.getValue().getWalLines());
            }
        }
        for(Long key : keysToRemove){
            bufferedMessages.remove(key);
        }
    }

    private void syncToFile(long versionNumber, long currentSequenceNumber) {

    }

    private void addEntryToDatabase(List<String> walLines) {
        for(String walLine: walLines){
            Util.loadStateFromLogLine(walLine, keyValuePair);
        }
    }

    static class BufferedMessagesContainer{
        private long versionNumber;
        private List<String> walLines;
        private int endLineNumber;

        public BufferedMessagesContainer(long versionNumber, List<String> walLines, int endLineNumber) {
            this.versionNumber = versionNumber;
            this.walLines = walLines;
            this.endLineNumber = endLineNumber;
        }

        public long getVersionNumber() {
            return versionNumber;
        }

        public void setVersionNumber(long versionNumber) {
            this.versionNumber = versionNumber;
        }

        public List<String> getWalLines() {
            return walLines;
        }

        public void setWalLines(List<String> walLines) {
            this.walLines = walLines;
        }

        public int getEndLineNumber() {
            return endLineNumber;
        }

        public void setEndLineNumber(int endLineNumber) {
            this.endLineNumber = endLineNumber;
        }

        @Override
        public String toString() {
            return "BufferedMessagesContainer{" +
                    "versionNumber=" + versionNumber +
                    ", walLines=" + walLines +
                    ", endLineNumber=" + endLineNumber +
                    '}';
        }
    }
}
