package RoadModel;

import Constants.TrafficMonitoringConstants.Conf;
import Util.config.Configuration;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.postgis.MultiLineString;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *  @author Alessandra Fais
 *  @version June 2019
 *
 *  The class defines a data structure containing information about the roads layer extracted from the shapefile.
 */
public class RoadGridList {
    private HashMap<String, ArrayList<SimpleFeature>> gridList = new HashMap<>();
    private String idKey;
    private String widthKey;

    /**
     *  Constructor.
     *  Read the shape-file and initialize the gridList data structure.
     *  @param path path of the shape-file
     *  @throws SQLException
     *  @throws IOException
     */
    public RoadGridList(Configuration config, String path) throws SQLException, IOException {
        gridList = read(path);

        idKey = config.getString(Conf.ROAD_FEATURE_ID_KEY);                  // attribute of the SimpleFeature containing the ID of the road
        widthKey = config.getString(Conf.ROAD_FEATURE_WIDTH_KEY, null); // width of the cells of the grid into which the map is divided
    }

    /**
     *  Given a point p (identified by coordinates X and Y on the map) return a road ID.
     *  @param p input coordinates
     *  @return road ID corresponding to p in the map described by the shapefile
     *  @throws SQLException
     */
    public int fetchRoadID(Point p) throws SQLException {
        int lastMiniRoadID = -2;

        Integer mapID_lon = (int)(p.getX()*10);
        Integer mapID_lan = (int)(p.getY()*10);
        String mapID = mapID_lan.toString() + "_" + mapID_lon.toString();
        double minD = Double.MAX_VALUE;
        int width = 0;

        int gridCount = 0;
        int roadCount = 0;

        // search for a key (coordinates <X, Y>) in the map equals to point p coordinates <p.X, p.Y>;
        // the point will be part of one or more than one road (road IDs are contained in the list
        // associated to the key coordinates
        for (Map.Entry<String, ArrayList<SimpleFeature>> grid : gridList.entrySet()) {
            gridCount++;
            String s = grid.getKey();

            if (mapID.equals(s)) {
                for (SimpleFeature feature: grid.getValue()) {
                    roadCount++;
                    int roadID = Integer.parseInt(feature.getAttribute(idKey).toString());
                    // System.out.println("RoadID: " + roadID);

                    if (widthKey != null) {
                        width = Integer.parseInt(feature.getAttribute(widthKey).toString());
                    } else {
                        width = 5;
                    }
                    if (width <= 0) width = 5;

                    String geoStr = feature.getDefaultGeometry().toString();
                    MultiLineString linearRing = new MultiLineString(geoStr);
                    ArrayList<Point> ps = new ArrayList<>();

                    for (int idx = 0; idx < linearRing.getLine(0).numPoints(); idx++) {
                        Point pt = new Point(linearRing.getLine(0).getPoint(idx).x, linearRing.getLine(0).getPoint(idx).y);
                        ps.add(pt);
                    }

                    // System.out.println("Points defining the polygon: " + ps.size());
                    int n = ps.size();
                    for (int i = 0; i < n - 1; i++) {
                        double distance = Polygon.pointToLine(ps.get(i).getX(), ps.get(i).getY(), ps.get(i+1).getX(), ps.get(i+1).getY(), p.getX(), p.getY())*111.2*1000;

                        if (distance < minD) {
                            minD = distance;
                            lastMiniRoadID = roadID;
                        }
                        if (distance < width / 2.0) {// * Math.sqrt(2.0)) {
                            // System.out.printf("\ngridCount:%2d  roadCount:%5d  LessWidth,dist=%7.3f ",gridCount,roadCount,distance);
                            return roadID;
                        }
                    }
                }
                // System.out.printf("\ngridCount:%2d  roadCount:%5d  Minimum   dist=%7.3f ",gridCount,roadCount,minD);
                //System.out.println("last_min_dist: " + minD + " < " + Math.sqrt(width * width + 10 * 10) + " ?");
                if (minD < Math.sqrt(width * width + 10 * 10)) {
                    return lastMiniRoadID;
                } else
                    return -1;
            }
        }

        return -1;
    }

    //------------------------------------------ private methods -------------------------------------------------------

    /**
     *  Check if it exists an entry associated to mapID in the gridList.
     *  @param mapID coordinates <X, Y>
     *  @return the list of SimpleFeatures if an entry associated to the key mapID already exists,
     *          null otherwise
     */
    private ArrayList<SimpleFeature> getGridByID(String mapID) {
        for (Map.Entry<String, ArrayList<SimpleFeature>> g : gridList.entrySet()) {
            if (g.getKey().equals(mapID)) {
                return g.getValue();
            }
        }
        return null;
    }

    /**
     *  Check if it exists an entry associated to mapID in the gridList.
     *  @param gridList hash map that maps coordinates to a list of SimpleFeatures
     *  @param mapID coordinates <X, Y>
     *  @return true if an entry associated to the key mapID already exists, false otherwise
     */
    private Boolean exists(HashMap<String, ArrayList<SimpleFeature>> gridList, String mapID) {
        for (Map.Entry<String, ArrayList<SimpleFeature>> g : gridList.entrySet()) {
            if (g.getKey().equals(mapID)) {
                return true;
            }
        }
        return false;
    }

    /**
     *  Read the shape-file and construct the gridList data structure.
     *  @param path shape-file path
     *  @return gridList data structure
     *  @throws IOException
     *  @throws SQLException
     */
    private HashMap<String, ArrayList<SimpleFeature>> read(String path) throws IOException, SQLException {
        File file = new File(path);
        // System.out.println("File: " + file);

        ShapefileDataStore shpDataStore = new ShapefileDataStore(file.toURI().toURL());
        shpDataStore.setCharset(Charset.forName("GBK"));
        // System.out.println("Shapefile data store: \n" + shpDataStore);

        // feature access
        String typeName = shpDataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = shpDataStore.getFeatureSource(typeName);
        FeatureCollection<SimpleFeatureType, SimpleFeature> result = featureSource.getFeatures();
        FeatureIterator<SimpleFeature> iterator = result.features();
        // System.out.println("Feature: " + typeName + "\nSource: " + featureSource.toString());
        // System.out.println("SimpleFeatureType: " + shpDataStore.getSchema());
        // System.out.println("Number of features: " + result.size());

        while(iterator.hasNext()) {
            // data Reader
            SimpleFeature feature = iterator.next();

            String geoStr = feature.getDefaultGeometry().toString();
            MultiLineString linearRing = new MultiLineString(geoStr);

            String mapID;
            if (feature.getAttributes().contains("MapID")) {
                mapID = feature.getAttribute("MapID").toString();
                // System.out.println("Case 1: mapID is " + mapID);
            } else {
                int len = linearRing.getLine(0).numPoints();
                Double centerX = (linearRing.getLine(0).getPoint(0).x + linearRing.getLine(0).getPoint(len-1).x) / 2 * 10;
                Double centerY = (linearRing.getLine(0).getPoint(0).y + linearRing.getLine(0).getPoint(len-1).y) / 2 * 10;
                mapID = (centerY.toString()).substring(0, 3) + "_" + (centerX.toString()).substring(0, 4);
                // System.out.println("Case 2: mapID is " + mapID);
            }

            if (!exists(gridList, mapID)) {
                ArrayList<SimpleFeature> roadList = new ArrayList<>();
                roadList.add(feature);
                gridList.put(mapID,roadList);
            } else {
                ArrayList<SimpleFeature> roadList = getGridByID(mapID);
                roadList.add(feature);
            }
        }

        iterator.close();
        shpDataStore.dispose();

        return gridList;
    }
}
