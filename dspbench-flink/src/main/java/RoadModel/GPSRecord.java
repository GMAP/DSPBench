package RoadModel;

/**
 *  @author Alessandra Fais
 *  @version June 2019
 *
 *  The class defines a Point data structure containing X and Y coordinates.
 */
class Point {
    double x;
    double y;

    Point() {
    }

    Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    double getX() {
        return x;
    }

    double getY() {
        return y;
    }
}

/**
 *  @author Alessandra Fais
 *  @version June 2019
 *
 *  The class defines a GPSRecord data structure containing X and Y coordinates,
 *  speed and direction.
 */
public class GPSRecord extends Point {
    private double speed;
    private int direction;

    public GPSRecord() {
    }

    public GPSRecord(double x, double y, double speed, int direction) {
        this.x = x;
        this.y = y;
        this.speed = speed;
        this.direction = direction;
    }

    public double getSpeed() {
        return speed;
    }

    public int getDirection() {
        return direction;
    }
}
