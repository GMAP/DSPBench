package com.streamer.examples.machineoutlier;

import java.io.Serializable;

public class MachineMetadata implements Serializable {
    private long timestamp;
    private String machineIP;

    /* values between [0, 1] */
    private double cpuUsage;
    private double memoryUsage;

    public MachineMetadata() {
    }

    public MachineMetadata(long timestamp, String machineIP, double cpuUsage, double memoryUsage) {
        this.timestamp = timestamp;
        this.machineIP = machineIP;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMachineIP() {
        return machineIP;
    }

    public void setMachineIP(String machineIP) {
        this.machineIP = machineIP;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    @Override
    public String toString() {
        return "MachineMetadata{" + "timestamp=" + timestamp + ", machineIP=" + machineIP + ", cpuUsage=" + cpuUsage + ", memoryUsage=" + memoryUsage + '}';
    }
}