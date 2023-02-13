package com.simple.database.database.replica.request;

import java.util.List;
public class ReplicaRequest {
    private List<String> walLines;
    private int sequenceNumber;
    public ReplicaRequest(List<String> walLines, int sequenceNumber) {
        this.walLines = walLines;
        this.sequenceNumber = sequenceNumber;
    }
}
