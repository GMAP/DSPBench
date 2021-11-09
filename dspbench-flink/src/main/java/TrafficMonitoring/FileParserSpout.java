package TrafficMonitoring;

import Constants.TrafficMonitoringConstants.*;
import Util.config.Configuration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  The spout is in charge of reading the input data file containing
 *  vehicle-traces, parsing it and generating the stream of records
 *  toward the MapMatchingBolt.
 */
public class FileParserSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(FileParserSpout.class);

    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;

    private int rate;
    private String city;

    private long t_start;
    private long generated;
    private long emitted;
    private long nt_execution;
    private long nt_end;
    private int par_deg;

    // state of the spout (contains parsed data)
    private ArrayList<String> vehicles;
    private ArrayList<Double> latitudes;
    private ArrayList<Double> longitudes;
    private ArrayList<Double> speeds;
    private ArrayList<Integer> bearings;

    /**
     * Constructor: it expects the city, the generation rate and the parallelism degree.
     * @param c city to monitor
     * @param gen_rate the spout generates tuples at the rate given by this parameter
     * @param p_deg source parallelism degree
     */
    FileParserSpout(String c, int gen_rate, int p_deg) {
        city = c;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        emitted = 0;            // total number of emitted tuples
        nt_execution = 0;       // number of executions of nextTuple() method

        vehicles = new ArrayList<>();
        latitudes = new ArrayList<>();
        longitudes = new ArrayList<>();
        speeds = new ArrayList<>();
        bearings = new ArrayList<>();
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        LOG.info("[Source] Started ({} replicas with rate {}).", par_deg, rate);

        t_start = System.nanoTime(); // spout start time in nanoseconds

        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;

        // set city trace file path
        String city_tracefile =  (city.equals(City.DUBLIN)) ?
                config.getString(Conf.SPOUT_DUBLIN) :
                config.getString(Conf.SPOUT_BEIJING);

        // save tuples as a state
        parse(city_tracefile);
    }

    @Override
    public void nextTuple() {
        int interval = 1000000000; // one second (nanoseconds)
        long t_init = System.nanoTime();

        for (int tuple_idx = 0; tuple_idx < vehicles.size(); tuple_idx++) {
            long t_now = System.nanoTime();
            if (emitted >= rate) {
                if (t_now - t_init <= interval)
                    active_delay(interval - (t_now - t_init));
                emitted = 0;
                t_init = System.nanoTime();
            }
            collector.emit(new Values(vehicles.get(tuple_idx), latitudes.get(tuple_idx), longitudes.get(tuple_idx),
                    speeds.get(tuple_idx), bearings.get(tuple_idx), System.nanoTime()));
            emitted++;
            generated++;
            active_delay((double) interval / rate);
        }
        nt_execution++;
        nt_end = System.nanoTime();
    }

    @Override
    public void close() {
        if (nt_execution == 0) nt_end = System.nanoTime();
        long t_elapsed = (nt_end - t_start) / 1000000;  // elapsed time in milliseconds

        System.out.println("[Source] execution time: " + t_elapsed +
                            " ms, generations: " + nt_execution +
                            ", generated: " + generated +
                            ", bandwidth: " + generated / (t_elapsed / 1000) +  // tuples per second
                            " tuples/s");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(Field.VEHICLE_ID, Field.LATITUDE, Field.LONGITUDE,
                        Field.SPEED, Field.BEARING, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------

    /**
     * Parse vehicles' traces and populate the state of the spout.
     * @param city_tracefile dataset containing vehicles' traces for the given city
     */
    private void parse(String city_tracefile) {
        try {
            Scanner scan = new Scanner(new File(city_tracefile));
            while (scan.hasNextLine()) {
                String[] fields = scan.nextLine().split(",");

                /*
                 * Beijing vehicle-trace dataset is used freely, with the following acknowledgement:
                 * “This code was obtained from research conducted by the University of Southern
                 * California’s Autonomous Networks Research Group, http://anrg.usc.edu“.
                 *
                 * Format of the dataset:
                 * vehicleID, date-time, latitude, longitude, speed, bearing (direction)
                 */
                if (city.equals(City.BEIJING) && fields.length >= 7) {
                    vehicles.add(fields[BeijingParsing.B_VEHICLE_ID_FIELD]);
                    latitudes.add(Double.valueOf(fields[BeijingParsing.B_LATITUDE_FIELD]));
                    longitudes.add(Double.valueOf(fields[BeijingParsing.B_LONGITUDE_FIELD]));
                    speeds.add(Double.valueOf(fields[BeijingParsing.B_SPEED_FIELD]));
                    bearings.add(Integer.valueOf(fields[BeijingParsing.B_DIRECTION_FIELD]));
                    generated++;

                    LOG.debug("[Source] Beijing Fields: {} ; {} ; {} ; {} ; {}",
                            fields[BeijingParsing.B_VEHICLE_ID_FIELD],
                            fields[BeijingParsing.B_LATITUDE_FIELD],
                            fields[BeijingParsing.B_LONGITUDE_FIELD],
                            fields[BeijingParsing.B_SPEED_FIELD],
                            fields[BeijingParsing.B_DIRECTION_FIELD]);
                }

                /*
                 * GPS data about buses across Dublin City are retrieved from Dublin City
                 * Council'traffic control.
                 * See https://data.gov.ie/dataset/dublin-bus-gps-sample-data-from-dublin-city-council-insight-project
                 *
                 * Format of the dataset:
                 * timestamp, lineID, direction, journeyPatternID, timeFrame, vehicleJourneyID, busOperator, congestion,
                 * longitude, latitude, delay, blockID, vehicleID, stopID, atStop
                 */
                if (city.equals(City.DUBLIN) && fields.length >= 15) {
                    vehicles.add(fields[DublinParsing.D_VEHICLE_ID_FIELD]);
                    latitudes.add(Double.valueOf(fields[DublinParsing.D_LATITUDE_FIELD]));
                    longitudes.add(Double.valueOf(fields[DublinParsing.D_LONGITUDE_FIELD]));
                    speeds.add(0.0); // speed values are not present in the used dataset
                    bearings.add(Integer.valueOf(fields[DublinParsing.D_DIRECTION_FIELD]));
                    generated++;

                    LOG.debug("[Source] Dublin Fields: {} ; {} ; {} ; {}",
                            fields[DublinParsing.D_VEHICLE_ID_FIELD],
                            fields[DublinParsing.D_LATITUDE_FIELD],
                            fields[DublinParsing.D_LONGITUDE_FIELD],
                            fields[DublinParsing.D_DIRECTION_FIELD]);
                }
            }
            scan.close();
            LOG.info("[Source] parsed dataset: " + generated + " tuples");
            generated = 0;
        } catch (FileNotFoundException | NullPointerException e) {
            LOG.error("The file {} does not exists", city_tracefile);
            throw new RuntimeException("The file '" + city_tracefile + "' does not exists");
        }
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;

        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
        LOG.debug("[Source] delay " + nsecs + " ns.");
    }
}
