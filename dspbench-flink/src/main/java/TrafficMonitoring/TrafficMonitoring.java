package TrafficMonitoring;

import Constants.BaseConstants;
import Constants.BaseConstants.Execution;
import Constants.TrafficMonitoringConstants;
import Constants.TrafficMonitoringConstants.City;
import Constants.TrafficMonitoringConstants.Component;
import Constants.TrafficMonitoringConstants.Conf;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.storm.wrappers.BoltWrapper;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  The topology entry class. The Storm compatible API is used in order to submit
 *  a Storm topology to Flink. The used Storm classes are replaced with their
 *  Flink counterparts in the Storm client code that assembles the topology.
 *
 *  See https://ci.apache.org/projects/flink/flink-docs-stable/dev/libs/storm_compatibility.html
 */
public class TrafficMonitoring {

    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);

    /**
     * Embed Storm operators in the Flink streaming program.
     * @param args command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() == 1 && params.get("help").equals(BaseConstants.HELP)) {
            String alert =
                    "In order to correctly run TrafficMonitoring app you can pass the following (optional) arguments:\n" +
                    "Optional arguments (default values are specified in tm.properties or defined as constants):\n" +
                    " city (accepted values {beijing, dublin})\n" +
                    " source parallelism degree\n" +
                    " map matching bolt parallelism degree\n" +
                    " speed calculator bolt parallelism degree\n" +
                    " sink parallelism degree\n" +
                    " source generation rate (default 1000 tuples/second)\n" +
                    " topology name (default TrafficMonitoring)\n" +
                    " execution mode (default local)";
            LOG.error(alert);
        } else {
            // load default configuration
            ParameterTool conf;
            try {
                String cfg = TrafficMonitoringConstants.DEFAULT_PROPERTIES;
                conf = ParameterTool.fromPropertiesFile(TrafficMonitoring.class.getResourceAsStream(cfg));
            } catch (IOException e) {
                LOG.error("Unable to load configuration file.", e);
                throw new RuntimeException("Unable to load configuration file.", e);
            }

            // parse command line arguments
            // if city is not a valid string then take the default value from properties file
            String city;
            if (params.get("city") != null && (params.get("city").equals(City.BEIJING) || params.get("city").equals(City.DUBLIN)))
                city = params.get("city");
            else
                city = conf.get(Conf.MAP_MATCHER_SHAPEFILE);
            int source_par_deg = params.getInt("nsource", conf.getInt(Conf.SPOUT_THREADS));
            int matcher_par_deg = params.getInt("nmatcher", conf.getInt(Conf.MAP_MATCHER_THREADS));
            int calculator_par_deg = params.getInt("ncalculator", conf.getInt(Conf.SPEED_CALCULATOR_THREADS));
            int sink_par_deg = params.getInt("nsink", conf.getInt(Conf.SINK_THREADS));

            // source generation rate (for tests)
            int gen_rate = params.getInt("rate", Execution.DEFAULT_RATE);

            String topology_name = params.get("toponame", TrafficMonitoringConstants.DEFAULT_TOPO_NAME);
            String ex_mode = params.get("mode", Execution.LOCAL_MODE);

            // create the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // add the configuration
            env.getConfig().setGlobalJobParameters(params);
            env.getConfig().setGlobalJobParameters(conf);

            // set the parallelism degree for all activities in the topology
            int pardeg = params.getInt("pardeg", conf.getInt(Conf.ALL_THREADS));
            if (pardeg != conf.getInt(Conf.ALL_THREADS)) {
                source_par_deg = pardeg;
                matcher_par_deg = pardeg;
                calculator_par_deg = pardeg;
                sink_par_deg = pardeg;
            }

            System.out.println("[main] Command line arguments parsed and configuration set.");

            // create the topology
            DataStream<Tuple6<String, Double, Double, Double, Integer, Long>> source =
                    env
                        .addSource(
                            new SpoutWrapper<Tuple6<String, Double, Double, Double, Integer, Long>>(
                                new FileParserSpout(city, gen_rate, source_par_deg)),
                            Component.SPOUT) // operator name
                        .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INT, Types.LONG))   // output type
                        .setParallelism(source_par_deg);

            System.out.println("[main] Spout created.");

            DataStream<Tuple3<Integer, Integer, Long>> map_matching_bolt =
                    source
                        .transform(
                            Component.MAP_MATCHER, // operator name
                            TypeExtractor.getForObject(new Tuple3<>(0, 0, 0L)), // output type
                            new BoltWrapper<>(new MapMatchingBolt(city, matcher_par_deg)))
                        .setParallelism(matcher_par_deg)
                        .keyBy(0); // group by roadID

            System.out.println("[main] Bolt MapMatcher created.");

            DataStream<Tuple4<Integer, Integer, Integer, Long>> speed_calc_bolt =
                    map_matching_bolt
                        .transform(
                            Component.SPEED_CALCULATOR, // operator name
                            TypeExtractor.getForObject(new Tuple4<>(0, 0, 0, 0L)), // output type
                            new BoltWrapper<>(new SpeedCalculatorBolt(calculator_par_deg)))
                        .setParallelism(calculator_par_deg);

            System.out.println("[main] Bolt SpeedCalculator created.");

            DataStream<Tuple4<Integer, Integer, Integer, Long>> sink =
                    speed_calc_bolt
                        .transform(
                            Component.SINK, // operator name
                            TypeExtractor.getForObject(new Tuple4<>(0, 0, 0, 0L)), // output type
                            new BoltWrapper<>(new ConsoleSink(sink_par_deg, gen_rate)))
                        .setParallelism(sink_par_deg);

            System.out.println("[main] Sink created.");

            System.out.println("[main] executing topology...");

            // print app info
            System.out.println("[SUMMARY] Executing TrafficMonitoring with parameters:\n" +
                            "* city: " + city + "\n" +
                            "* source parallelism degree: " + source_par_deg + "\n" +
                            "* map-match parallelism degree: " + matcher_par_deg + "\n" +
                            "* calculator parallelism degree: " + calculator_par_deg + "\n" +
                            "* sink parallelism degree: " + sink_par_deg + "\n" +
                            "* rate: " + gen_rate + "\n" +
                            "Topology: source -> map-matcher -> speed-calculator -> sink");

            env.execute(topology_name);
        }
    }
}