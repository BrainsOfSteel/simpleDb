package com.simple.database.database.request;

public class AddReplicaRequest {
    private String restEndPoint;
    private String fileName;

    public AddReplicaRequest(String restEndPoint, String fileName) {
        this.restEndPoint = restEndPoint;
        this.fileName = fileName;
    }

    public String getRestEndPoint() {
        return restEndPoint;
    }

    public void setRestEndPoint(String restEndPoint) {
        this.restEndPoint = restEndPoint;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
