package com.simple.database.database.replica.request;

import java.util.List;
public class ReplicaRequest {
    private List<String> walLines;
    private int sequenceNumber;
    private int endLineNumber;

    public ReplicaRequest(List<String> walLines, int sequenceNumber, int endLineNumber) {
        this.walLines = walLines;
        this.sequenceNumber = sequenceNumber;
        this.endLineNumber = endLineNumber;
    }

    public List<String> getWalLines() {
        return walLines;
    }

    public void setWalLines(List<String> walLines) {
        this.walLines = walLines;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public int getEndLineNumber() {
        return endLineNumber;
    }

    public void setEndLineNumber(int endLineNumber) {
        this.endLineNumber = endLineNumber;
    }

    @Override
    public String toString() {
        return "ReplicaRequest{" +
                "walLines=" + walLines +
                ", sequenceNumber=" + sequenceNumber +
                ", endLineNumber=" + endLineNumber +
                '}';
    }
}
