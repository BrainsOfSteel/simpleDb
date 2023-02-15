package com.simple.database.database.master;

import com.simple.database.database.replica.request.ReplicaRequest;
import com.simple.database.database.utils.Util;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SyncReplicasRunnable implements Runnable{

    private String replicaStateFileName;
    private String writeAheadFileName;
    private int maxBatchSize;
    private String replicaEndPointDetails;
    private RestTemplate restTemplate;
    private int bufferTime = 300;//in ms
    private int MILLIS_TO_NANOS = 1000000;
    private AtomicLong countRunningReplicaThreads;
    private AtomicBoolean cleanupSignal;

    public SyncReplicasRunnable(String replicaStateFileName, String writeAheadFileName, String replicaHostDetails, int maxBatchSize, AtomicLong countRunningReplicaThreads,
                                AtomicBoolean cleanupSignal) {
        this.replicaStateFileName = replicaStateFileName;
        this.writeAheadFileName = writeAheadFileName;
        this.maxBatchSize = maxBatchSize;
        this.restTemplate = new RestTemplate();
        this.replicaEndPointDetails = replicaHostDetails;
        this.countRunningReplicaThreads = countRunningReplicaThreads;
        this.cleanupSignal = cleanupSignal;
    }

    @Override
    public void run() {
        int currentSeqNumber = 0;
        int endLineNumber = -1;
        boolean cleanupSignalReceived = false;
        try (FileWriter fw = new FileWriter(replicaStateFileName, true)) {
            try (BufferedReader br = new BufferedReader(new FileReader(replicaStateFileName))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    String lineArr[] = line.split(Util.KEY_VALUE_DELIMITER);
                    if(!line.contains(Util.KEY_VALUE_DELIMITER)){
                        continue;
                    }
                    currentSeqNumber = Integer.parseInt(lineArr[0]);
                    endLineNumber = Integer.parseInt(lineArr[1]);
                }
            }
            try (BufferedReader walReader = new BufferedReader(new FileReader(writeAheadFileName))) {
                String line = null;
                int count = 0;
                while (count <= endLineNumber) {
                    count++;
                    walReader.readLine();
                }

                while (true) {
                    if(cleanupSignal.get()){
                        cleanupSignalReceived = true;
                        System.out.println("Stopping this thread " + Thread.currentThread().getName() + " for clean up of WAL of master");
                        break;
                    }
                    List<String> lines = new ArrayList<>();
                    int collectedLines = 0;
                    boolean flag = false;
                    long startTime = System.nanoTime();
                    long endTime = startTime;
                    long timeElapsed = 0L;
                    while (collectedLines < maxBatchSize && (line = walReader.readLine()) != null && (timeElapsed = (endTime-startTime)) < bufferTime * MILLIS_TO_NANOS) {
                        collectedLines++;
                        lines.add(line);
                        flag = true;
                        endTime = System.nanoTime();
                    }
                    if(!flag){
                        continue;
                    }
                    System.out.println("condition met" + (collectedLines < maxBatchSize) +" line = " + line + " timeElapsed = "+ timeElapsed);
                    currentSeqNumber++;
                    endLineNumber = endLineNumber < 0 ? collectedLines : endLineNumber  + collectedLines;
                    sendToTheReplicaHost(currentSeqNumber, endLineNumber, lines);
                    fw.write(currentSeqNumber + Util.KEY_VALUE_DELIMITER + endLineNumber + Util.ENTRY_DELIMITER);
                    fw.flush();
                }
            }
            countRunningReplicaThreads.decrementAndGet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        cleanupIfRequired(cleanupSignalReceived);
    }

    private void cleanupIfRequired(boolean cleanupSignalReceived) {
        if(!cleanupSignalReceived){
            return;
        }
        try{
            Files.deleteIfExists(Path.of(replicaStateFileName));
        }catch (Exception e){
            System.out.println("Unable to delete the state file. Not a catatrophic failure");
            e.printStackTrace();
        }
    }

    private void sendToTheReplicaHost(int currentSeqNumber, int endNumber, List<String> lines) {
        ReplicaRequest replicaRequest = new ReplicaRequest(lines, currentSeqNumber, endNumber);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<ReplicaRequest> request = new HttpEntity<>(replicaRequest, httpHeaders);
        restTemplate.postForObject(replicaEndPointDetails, request, String.class);
        System.out.println("seqNumber =" + currentSeqNumber + " endNumber = "+endNumber + " size =" + lines.size() + " lines = " + lines);
    }
}
