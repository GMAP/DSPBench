package spark.streaming.model.gis;

import java.io.Serializable;

public class Point implements Serializable {
    protected double x;
    protected double y;
    
    public Point() {
    }
    
    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}