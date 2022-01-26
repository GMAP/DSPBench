package storm.applications.model.gis;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.postgis.MultiLineString;
import storm.applications.constants.TrafficMonitoringConstants.Conf;
import storm.applications.util.config.Configuration;

public class RoadGridList {
    private HashMap<String, RoadList> gridList = new HashMap<>();
    private String idKey;
    private String widthKey;
    
    public RoadGridList(Configuration config, String path) throws SQLException, IOException {
        gridList = read(path);
        
        idKey    = config.getString(Conf.ROAD_FEATURE_ID_KEY);
        widthKey = config.getString(Conf.ROAD_FEATURE_WIDTH_KEY, null);
    }
    
    public RoadList getGridByID(String mapId) {
        for (Map.Entry<String, RoadList> g : gridList.entrySet()) {
            if (g.getKey().equals(mapId)) {
                return g.getValue();
            }
        }		   
        return null;		   
    }

    public Boolean isExits(HashMap<String, RoadList> gridList, String mapId) {
        for (Map.Entry<String, RoadList> g : gridList.entrySet()) {
            if (g.getKey().equals(mapId)) {
                return true;
            }
        }
        return false;
    }

    private HashMap<String, RoadList> read(String path) throws IOException, SQLException {
        File file = new File(path);
        
        ShapefileDataStore shpDataStore = new ShapefileDataStore(file.toURI().toURL());
        shpDataStore.setCharset(Charset.forName("GBK"));

        //Feature Access
        String typeName = shpDataStore.getTypeNames()[0];  
        FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) shpDataStore.getFeatureSource(typeName);  
        FeatureCollection<SimpleFeatureType, SimpleFeature> result = featureSource.getFeatures();  
        FeatureIterator<SimpleFeature> iterator = result.features();  
        
        while(iterator.hasNext()) {  
            //Data Reader
            SimpleFeature feature = iterator.next();  

            String geoStr = feature.getDefaultGeometry().toString();			
            MultiLineString linearRing = new MultiLineString(geoStr);

            String mapID;
            if (feature.getAttributes().contains("MapID")) {
                mapID = feature.getAttribute("MapID").toString();
            } else {
                int len = linearRing.getLine(0).numPoints();
                Double centerX = (linearRing.getLine(0).getPoint(0).x+linearRing.getLine(0).getPoint(len-1).x)/2*10;
                Double centerY = (linearRing.getLine(0).getPoint(0).y+linearRing.getLine(0).getPoint(len-1).y)/2*10;
                mapID = (centerY.toString()).substring(0, 3)+"_"+ (centerX.toString()).substring(0, 4);
            }

            if (!isExits(gridList, mapID)) {
                RoadList roadList = new RoadList();
                roadList.add(feature);
                gridList.put(mapID,roadList);
            } else {
                RoadList roadList = getGridByID(mapID);
                roadList.add(feature); 		
            }
        }
        
        iterator.close(); 
        shpDataStore.dispose();

        return gridList;
    }

    public int fetchRoadID(Point p) throws SQLException {
        int lastMiniRoadID = -2;

        Integer mapID_lon = (int)(p.getX()*10);
        Integer mapID_lan = (int)(p.getY()*10);
        String mapID = mapID_lan.toString()+"_" +mapID_lon.toString();
        double minD = Double.MAX_VALUE;
        int width = 0;

        int gridCount = 0;
        int roadCount = 0;
        
        for (Map.Entry<String, RoadList> grid : gridList.entrySet()) {
            gridCount++;
            String s = grid.getKey();

            if (mapID.equals(s)) {
                for (SimpleFeature feature: grid.getValue()) {
                    roadCount++;
                    int returnRoadID = Integer.parseInt(feature.getAttribute(idKey).toString());
                    
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

                    int n = ps.size();
                    for (int i = 0; i < n - 1; i++) {
                        double distance = Polygon.pointToLine(ps.get(i).getX(), ps.get(i).getY(), ps.get(i+1).getX(), ps.get(i+1).getY(), p.getX(), p.getY())*111.2*1000;

                        if (distance < width) {
                            //System.out.printf("\ngridCount:%2d  roadCount:%5d  LessWidth,dist=%7.3f ",gridCount,roadCount,distance);
                            return returnRoadID;
                        } else if (distance < minD) {
                            minD = distance;
                            lastMiniRoadID = returnRoadID;
                        }
                    }
                }
                
                //System.out.printf("\ngridCount:%2d  roadCount:%5d  Minimum   dist=%7.3f ",gridCount,roadCount,minD);
                
                if (minD < Math.sqrt(Math.pow(width, 2) + Math.pow(10, 2))) {
                    return lastMiniRoadID;
                } else 
                    return -1;
            }
        }
        
        return -1;
    }
    
    public class RoadList extends ArrayList<SimpleFeature> {
        SimpleFeature road;
        
        RoadList(){}
        
        RoadList(SimpleFeature road) {
            this.road = road;
        }
    }
}
