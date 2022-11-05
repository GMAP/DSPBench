package flink.application.trafficmonitoring.gis;

import flink.application.trafficmonitoring.collections.FixedSizeQueue;

public class Road {
    private final long roadID;
    private final FixedSizeQueue<Integer> roadSpeed;
    private int averageSpeed;
    private int count;

    public Road(long roadID) {
        this.roadID = roadID;
        this.roadSpeed = new FixedSizeQueue<>(30);
    }

    public long getRoadID() {
        return roadID;
    }

    public int getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(int averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    
    public void incrementCount() {
        this.count++;
    }

    public FixedSizeQueue<Integer> getRoadSpeed() {
        return roadSpeed;
    }

    public boolean addRoadSpeed(int speed) {
        return roadSpeed.add(speed);
    }
    
    public int getRoadSpeedSize() {
        return roadSpeed.size();
    }
}
