package SpikeDetection;

import Constants.BaseConstants;
import Constants.SpikeDetectionConstants;
import Constants.BaseConstants.*;
import Constants.SpikeDetectionConstants.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.storm.wrappers.BoltWrapper;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class SpikeDetection {

    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);

    /**
     * Embed Storm operators in the Flink streaming program.
     * @param args command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() == 1 && params.get("help").equals(BaseConstants.HELP)) {
            String alert =
                    "In order to correctly run SpikeDetection app you can pass the following (optional) arguments:\n" +
                    "Optional arguments (default values are specified in sd.properties or defined as constants):\n" +
                    " file path\n" +
                    " source parallelism degree\n" +
                    " average calculator bolt parallelism degree\n" +
                    " spike detector bolt parallelism degree\n" +
                    " sink parallelism degree\n" +
                    " source generation rate (default -1, generate at the max possible rate)\n" +
                    " topology name (default SpikeDetection)\n" +
                    " execution mode (default local)";
            LOG.error(alert);
        } else {
            // load the configuration
            String cfg = SpikeDetectionConstants.DEFAULT_PROPERTIES;
            ParameterTool conf = ParameterTool.fromPropertiesFile(SpikeDetection.class.getResourceAsStream(cfg));

            // parse command line arguments
            String file_path = params.get("file", conf.get(Conf.SPOUT_PATH));
            int source_par_deg = params.getInt("nsource", conf.getInt(Conf.SPOUT_THREADS));
            int average_par_deg = params.getInt("naverage", conf.getInt(Conf.MOVING_AVERAGE_THREADS));
            int detector_par_deg = params.getInt("ndetector", conf.getInt(Conf.SPIKE_DETECTOR_THREADS));
            int sink_par_deg = params.getInt("nsink", conf.getInt(Conf.SINK_THREADS));

            // source generation rate (for tests)
            int gen_rate = params.getInt("rate", Execution.DEFAULT_RATE);

            String topology_name = params.get("toponame", SpikeDetectionConstants.DEFAULT_TOPO_NAME);
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
                average_par_deg = pardeg;
                detector_par_deg = pardeg;
                sink_par_deg = pardeg;
            }

            System.out.println("[main] Command line arguments parsed and configuration set.");

            // create the topology
            DataStream<Tuple3<String, Double, Long>> source =
                    env
                        .addSource(
                            new SpoutWrapper<Tuple3<String, Double, Long>>(
                                    new FileParserSpout(file_path, gen_rate, source_par_deg)),
                            Component.SPOUT) // operator name
                        .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG))   // output type
                        .setParallelism(source_par_deg)
                        .keyBy(0);

            System.out.println("[main] Spout created.");

            DataStream<Tuple4<String, Double, Double, Long>> moving_average_bolt =
                    source
                        .transform(
                            Component.MOVING_AVERAGE, // operator name
                            TypeExtractor.getForObject(new Tuple4<>("", 0.0, 0.0, 0L)), // output type
                            new BoltWrapper<>(new MovingAverageBolt(average_par_deg)))
                        .setParallelism(average_par_deg);

            System.out.println("[main] Bolt MovingAverage created.");

            DataStream<Tuple4<String, Double, Double, Long>> spike_detector_bolt =
                    moving_average_bolt
                        .transform(
                            Component.SPIKE_DETECTOR, // operator name
                            TypeExtractor.getForObject(new Tuple4<>("", 0.0, 0.0, 0L)), // output type
                            new BoltWrapper<>(new SpikeDetectorBolt(detector_par_deg)))
                        .setParallelism(detector_par_deg);

            System.out.println("[main] Bolt SpikeDetector created.");

            DataStream<Tuple4<String, Double, Double, Long>> sink =
                    spike_detector_bolt
                        .transform(
                            Component.SINK, // operator name
                            TypeExtractor.getForObject(new Tuple4<>("", 0.0, 0.0, 0L)), // output type
                            new BoltWrapper<>(new ConsoleSink(sink_par_deg, gen_rate)))
                        .setParallelism(sink_par_deg);

            System.out.println("[main] Sink created.");

            System.out.println("[main] executing topology...");

            // print app info
            System.out.println("[SUMMARY] Executing SpikeDetection with parameters:\n" +
                    "* file: " + file_path + "\n" +
                    "* source parallelism degree: " + source_par_deg + "\n" +
                    "* moving-average parallelism degree: " + average_par_deg + "\n" +
                    "* detector parallelism degree: " + detector_par_deg + "\n" +
                    "* sink parallelism degree: " + sink_par_deg + "\n" +
                    "* rate: " + gen_rate + "\n" +
                    "Topology: source -> moving-average -> detector -> sink");

            env.execute(topology_name);
        }
    }
}
