package RoadModel;

import java.util.ArrayList;

/**
 *  @author Alessandra Fais
 *  @version June 2019
 *
 *  The class defines a polygon given a list of points.
 */
class Polygon {
    private static final int EARTH_RADIUS = 6378137;

    private ArrayList<Point> points;
    private double x_min;
    private double x_max;
    private double y_min;
    private double y_max;
    private int count;          // number of points
    private double distance_min = 10 / 111.2 * 1000;

    Polygon(ArrayList<Point> points) {
        this.points = points;
        this.count = points.size();

        this.x_min = Double.MAX_VALUE;
        this.x_max = Double.MIN_VALUE;
        this.y_min = Double.MAX_VALUE;
        this.y_max = Double.MIN_VALUE;

        for (Point p : points) {
            if (p.getX() > this.x_max)
                this.x_max = p.getX();
            if (p.getX() < this.x_min)
                this.x_min = p.getX();
            if (p.getY() > this.y_max)
                this.y_max = p.getY();
            if (p.getY() < this.y_min)
                this.y_min = p.getY();
        }
    }

    /**
     *  Check if the point p is contained in the polygon.
     *  @param p point
     *  @return true if p is contained in the polygon, false otherwise
     */
    public Boolean contains(Point p) {
        if(p.getX() >= x_max || p.getX() < x_min || p.getY() >= y_max || p.getY() < y_min)
            return false;

        int cn = 0;
        int n = points.size();
        for (int i = 0; i < n - 1; i++) {
            if (points.get(i).getY() != points.get(i+1).getY() && !((p.getY() < points.get(i).getY())
                    && (p.getY() < points.get(i+1).getY())) && !((p.getY() > points.get(i).getY())
                    && (p.getY() > points.get(i+1).getY()))) {
                double uy = 0;
                double by = 0;
                double ux = 0;
                double bx = 0;
                int dir = 0;

                if (points.get(i).getY() > points.get(i+1).getY()) {
                    uy = points.get(i).getY();
                    by = points.get(i+1).getY();
                    ux = points.get(i).getX();
                    bx = points.get(i+1).getX();
                    dir = 0;    // downward
                } else {
                    uy = points.get(i+1).getY();
                    by = points.get(i).getY();
                    ux = points.get(i+1).getX();
                    bx = points.get(i).getX();
                    dir = 1;    // upward
                }

                double tx = 0;
                if (ux != bx){
                    double k = (uy - by) / (ux - bx);
                    double b = ((uy - k * ux) + (by - k * bx)) / 2;
                    tx = (p.getY() - b) / k;
                } else {
                    tx = ux;
                }

                if (tx > p.getX()) {
                    if(dir == 1 && p.getY() != points.get(i+1).getY())
                        cn++;
                    else if(p.getY() != points.get(i).getY())
                        cn++;
                }
            }
        }
        return cn % 2 != 0;
    }

    /**
     *  Check if it exists a match between the point's coordinates and the area of the road.
     *  @param p point
     *  @param roadWidth width of the road
     *  @return true if the point falls inside the road polygon area, false otherwise
     */
    public boolean matchToRoad(Point p, int roadWidth) {
        int n = points.size();
        for (int i = 0; i < n - 1; i++) {
            double distance = Math.sqrt(Math.pow(points.get(i).getY() - p.getY(), 2) + Math.pow(points.get(i).getX() - p.getX(), 2));

            if (distance < roadWidth / 2.0 * Math.sqrt(2.0))
                return true;
        }
        return false;
    }

    /**
     *  Check if it exists a match between the point's coordinates and the area of the road.
     *  @param p point
     *  @param roadWidth width of the road
     *  @param ps points defining the road polygon
     *  @return true if the point falls inside the road polygon area, false otherwise
     */
    public boolean matchToRoad(Point p, int roadWidth, ArrayList<Point> ps) {
        double minD=Double.MAX_VALUE;
        int n = ps.size();
        for (int i = 0; i < n - 2; i++) {
            double distance = Polygon.pointToLine(ps.get(i).getX(), ps.get(i).getY(), ps.get(i+1).getX(), ps.get(i+1).getY(), p.getX(), p.getY())*111.2*1000 ;

            if (distance < minD)
                minD = distance;

            //System.out.println("distance = " + distance);

            if (distance < roadWidth / 2.0 * Math.sqrt(2.0))
                return true;
        }
        return false;
    }

    public static double distancePointToLine(double x1,	double y1, double x2, double y2,double x, double y) {
        if (y1 == y2) {
            if (Math.min(x1, x2) < x && Math.max(x1, x2) > x) {     // x included between x1 and x2
                return Math.abs(computeD(y1, x, y, x));
            } else {                                                // x excluded
                return Math.min(computeD(y, x, y1, x1), computeD(y, x, y2, x2));
            }
        }

        if (x1 == x2) {
            if (Math.min(y1, y2) < y && Math.max(y1, y2) > y) {     // y included between y1 and y2
                return computeD(y, x1, y, x);
            } else {                                                // y excluded
                return Math.min(computeD(y, x, y1, x1), computeD(y, x, y2, x2));
            }
        }

        double k = (y2 - y1) / (x2 - x1);
        double tempX = (Math.pow(k, 2.0) * x1 + k * (y - y1) + x) / (Math.pow(k, 2.0) + 1.0);
        double tempY = k * (tempX - x1) + y1;

        if (tempX < -180 || tempX > 180 || tempY < -90 || tempY > 90) {
            return Math.min(computeD(y, x, y1, x1), computeD(y, x, y2, x2));
        }

        double tempDis1 = (computeD(tempY, tempX, y1, x1) + computeD(tempY, tempX, y2, x2));
        double tempDis2 = computeD(y1, x1, y2, x2);

        if ((tempDis1 - tempDis2) < 0.001) {
            return (computeD(tempY, tempX, y, x));
        } else {
            return Math.min(computeD(y, x, y1, x1), computeD(y, x, y2, x2));
        }
    }

    static double pointToLine(double x1, double y1, double x2, double y2, double x0, double y0) {
        double space = 0;
        double a, b, c;

        a = Polygon.lineSpace(x1, y1, x2, y2);
        b = lineSpace(x1, y1, x0, y0);
        c = lineSpace(x2, y2, x0, y0);

        if (c <= 0.000001 || b <= 0.000001) {
            space = 0;
            return space;
        }
        if (a <= 0.000001) {
            space = b;
            return space;
        }
        if (c * c >= a * a + b * b) {
            space = b;
            return space;
        }
        if (b * b >= a * a + c * c) {
            space = c;
            return space;
        }

        double p = (a + b + c) / 2;
        double s = Math.sqrt(p * (p - a) * (p - b) * (p - c));
        space = 2 * s / a;
        return space;
    }

    static double lineSpace(double x1, double y1, double x2, double y2) {
        double lineLength = 0;
        lineLength = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
        return lineLength;
    }

    static double computeD(double lat_a, double lng_a, double lat_b, double lng_b) {
        double radLat1 = (lat_a * Math.PI / 180.0);
        double radLat2 = (lat_b * Math.PI / 180.0);
        double a = radLat1 - radLat2;
        double b = (lng_a - lng_b) * Math.PI / 180.0;
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));

        s = s * EARTH_RADIUS;
        return s;
    }
}

/**
 *  @author Alessandra Fais
 *  @version June 2019
 *
 *  The class defines a sector as a polygon defined by a list of points
 *  along with an ID and the road width property.
 */
public class Sector extends Polygon {
    private int id;
    private int roadWidth;
    private String mapID;

    public Sector(ArrayList<Point> points, int id) {
        super(points);
        this.id = id;
    }

    public Sector(ArrayList<Point> points, int id, int roadWidth, String mapID) {
        super(points);
        this.id = id;
        this.roadWidth=roadWidth;
        this.mapID = mapID;
    }

    public int getID() {
        return this.id;
    }

    public int getroadWidth() {
        return this.roadWidth;
    }

    public String getMapID() {
        return this.mapID;
    }
}