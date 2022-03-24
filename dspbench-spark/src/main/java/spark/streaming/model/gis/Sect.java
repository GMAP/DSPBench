package spark.streaming.model.gis;

import java.io.Serializable;
import java.util.ArrayList;

public class Sect extends Polygon implements Serializable {
    private int id;
    private int roadWidth;
    private String mapID;

    public Sect(ArrayList<Point> points, int id) {
        super(points);
        this.id = id;
    }

    public Sect(ArrayList<Point> points, int id, int roadWidth, String mapID) {
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