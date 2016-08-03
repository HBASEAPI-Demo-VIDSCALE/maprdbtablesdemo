package com.mapr.demo;

/**
 * Created by mlalapet on 8/3/16.
 */
public class PropertyGlobalHour {
    private long epochStart;
    private int accountId;
    private int groupId;
    private String property;
    private String serviceType;
    private String flowDir;
    private double bytes;
    private double requests;
    private long connections;
    private double avgFbl;
    private double chitRatio;
    private long uniqueVisitors;

    public long getEpochStart() {
        return epochStart;
    }

    public void setEpochStart(long epochStart) {
        this.epochStart = epochStart;
    }

    public int getAccountId() {
        return accountId;
    }

    public void setAccountId(int accountId) {
        this.accountId = accountId;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getFlowDir() {
        return flowDir;
    }

    public void setFlowDir(String flowDir) {
        this.flowDir = flowDir;
    }

    public double getBytes() {
        return bytes;
    }

    public void setBytes(double bytes) {
        this.bytes = bytes;
    }

    public double getRequests() {
        return requests;
    }

    public void setRequests(double requests) {
        this.requests = requests;
    }

    public long getConnections() {
        return connections;
    }

    public void setConnections(long connections) {
        this.connections = connections;
    }

    public double getAvgFbl() {
        return avgFbl;
    }

    public void setAvgFbl(double avgFbl) {
        this.avgFbl = avgFbl;
    }

    public double getChitRatio() {
        return chitRatio;
    }

    public void setChitRatio(double chitRatio) {
        this.chitRatio = chitRatio;
    }

    public long getUniqueVisitors() {
        return uniqueVisitors;
    }

    public void setUniqueVisitors(long uniqueVisitors) {
        this.uniqueVisitors = uniqueVisitors;
    }
}
