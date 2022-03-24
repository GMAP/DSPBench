package FraudDetection;

import Constants.BaseConstants;
import Constants.BaseConstants.Execution;
import Constants.FraudDetectionConstants;
import Constants.FraudDetectionConstants.Component;
import Constants.FraudDetectionConstants.Conf;
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
public class FraudDetection {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    /**
     *  Embed Storm operators in the Flink streaming program.
     *  @param args command line arguments
     *  @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() == 1 && params.get("help").equals(BaseConstants.HELP)) {
            String alert =
                    "In order to correctly run FraudDetection app you can pass the following (optional) arguments:\n" +
                    "Optional arguments (default values are specified in fd.properties or defined as constants):\n" +
                    " file path\n" +
                    " source parallelism degree\n" +
                    " bolt parallelism degree\n" +
                    " sink parallelism degree\n" +
                    " source generation rate (default -1, generate at the max possible rate)\n" +
                    " topology name (default FraudDetection)\n" +
                    " execution mode (default local)";
            LOG.error(alert);
        } else {
            // load the configuration
            String cfg = FraudDetectionConstants.DEFAULT_PROPERTIES;
            ParameterTool conf = ParameterTool.fromPropertiesFile(FraudDetection.class.getResourceAsStream(cfg));

            // parse command line arguments
            String file_path = params.get("file", conf.get(Conf.SPOUT_PATH));
            int source_par_deg = params.getInt("nsource", conf.getInt(Conf.SPOUT_THREADS));
            int predictor_par_deg = params.getInt("npredictor", conf.getInt(Conf.PREDICTOR_THREADS));
            int sink_par_deg = params.getInt("nsink", conf.getInt(Conf.SINK_THREADS));

            // source generation rate (for tests)
            int gen_rate = params.getInt("rate", Execution.DEFAULT_RATE);

            String topology_name = params.get("toponame", FraudDetectionConstants.DEFAULT_TOPO_NAME);
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
                predictor_par_deg = pardeg;
                sink_par_deg = pardeg;
            }

            System.out.println("[main] Command line arguments parsed and configuration set.");

            // create the topology
            DataStream<Tuple3<String, String, Long>> source =
                env
                    .addSource(
                        new SpoutWrapper<Tuple3<String, String, Long>>(
                                new FileParserSpout(file_path, ",", gen_rate, source_par_deg)),
                        Component.SPOUT) // operator name
                    .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))   // output type
                    .setParallelism(source_par_deg)
                    .keyBy(0);

            System.out.println("[main] Spout created.");

            DataStream<Tuple4<String, Double, String, Long>> fraud_predictor =
                source
                    .transform(
                        Component.PREDICTOR, // operator name
                        TypeExtractor.getForObject(new Tuple4<>("", 0.0, "", 0L)), // output type
                        new BoltWrapper<>(new FraudPredictorBolt(predictor_par_deg)))
                    .setParallelism(predictor_par_deg);

            System.out.println("[main] Bolt created.");

            DataStream<Tuple4<String, Double, String, Long>> sink =
                fraud_predictor
                    .transform(
                        Component.SINK, // operator name
                        TypeExtractor.getForObject(new Tuple4<>("", 0.0, "", 0L)), // output type
                        new BoltWrapper<>(new ConsoleSink(sink_par_deg, gen_rate)))
                    .setParallelism(sink_par_deg);

            System.out.println("[main] Sink created.");

            System.out.println("[main] Executing topology...");

            // print app info
            System.out.println("[SUMMARY] Executing FraudDetection with parameters:\n" +
                                "* file: " + file_path + "\n" +
                                "* source parallelism degree: " + source_par_deg + "\n" +
                                "* predictor parallelism degree: " + predictor_par_deg + "\n" +
                                "* sink parallelism degree: " + sink_par_deg + "\n" +
                                "* rate: " + gen_rate + "\n" +
                                "Topology: source -> predictor -> sink");

            env.execute(topology_name);
        }
    }
}
