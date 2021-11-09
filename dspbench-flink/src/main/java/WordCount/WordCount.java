package WordCount;

import Constants.BaseConstants;
import Constants.BaseConstants.Execution;
import Constants.WordCountConstants;
import Constants.WordCountConstants.Component;
import Constants.WordCountConstants.Conf;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class WordCount {

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    /**
     * Embed Storm operators in the Flink streaming program.
     * @param args command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() == 1 && params.get("help").equals(BaseConstants.HELP)) {
            String alert =
                    "In order to correctly run WordCount app you can pass the following (optional) arguments:\n" +
                    "Optional arguments (default values are specified in wc.properties or defined as constants):\n" +
                    " source type (assume value in {file, generator})\n" +
                    " file path (valid only if the source type is file)\n" +
                    " source parallelism degree\n" +
                    " splitter bolt parallelism degree\n" +
                    " counter bolt parallelism degree\n" +
                    " sink parallelism degree\n" +
                    " source generation rate (default -1, generate at the max possible rate)\n" +
                    " topology name (default WordCount)\n" +
                    " execution mode (default local)";
            LOG.error(alert);
        } else {
            // load the configuration
            String cfg = WordCountConstants.DEFAULT_PROPERTIES;
            ParameterTool conf = ParameterTool.fromPropertiesFile(WordCount.class.getResourceAsStream(cfg));

            // parse command line arguments
            String source_type = params.get("sourcetype", Conf.FILE_SOURCE);
            String file_path = null;
            if (source_type.equals(Conf.FILE_SOURCE))
                file_path = params.get("filepath", conf.get(Conf.SPOUT_PATH));
            int source_par_deg = params.getInt("nsource", conf.getInt(Conf.SPOUT_THREADS));
            int splitter_par_deg = params.getInt("nsplitter", conf.getInt(Conf.SPLITTER_THREADS));
            int counter_par_deg = params.getInt("ncounter", conf.getInt(Conf.COUNTER_THREADS));
            int sink_par_deg = params.getInt("nsink", conf.getInt(Conf.SINK_THREADS));

            // source generation rate (for tests)
            int gen_rate = params.getInt("rate", Execution.DEFAULT_RATE);

            String topology_name = params.get("toponame", WordCountConstants.DEFAULT_TOPO_NAME);
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
                splitter_par_deg = pardeg;
                counter_par_deg = pardeg;
                sink_par_deg = pardeg;
            }

            System.out.println("[main] Command line arguments parsed and configuration set.");

            // create the topology
            DataStream<Tuple2<String, Long>> source =
                    env
                        .addSource(
                            new SpoutWrapper<Tuple2<String, Long>>(
                                    new FileParserSpout(source_type, file_path, gen_rate, source_par_deg)),
                            Component.SPOUT) // operator name
                        .returns(Types.TUPLE(Types.STRING, Types.LONG))   // output type
                        .setParallelism(source_par_deg);

            System.out.println("[main] Spout created.");

            DataStream<Tuple2<String, Long>> splitter_bolt =
                    source
                        .transform(
                            Component.SPLITTER, // operator name
                            TypeExtractor.getForObject(new Tuple2<>("", 0L)), // output type
                            new BoltWrapper<>(new SplitterBolt(splitter_par_deg)))
                        .setParallelism(splitter_par_deg)
                        .keyBy(0); // group by word

            System.out.println("[main] Bolt Splitter created.");

            DataStream<Tuple3<String, Long, Long>> counter_bolt =
                    splitter_bolt
                        .transform(
                            Component.COUNTER, // operator name
                            TypeExtractor.getForObject(new Tuple3<>("", 0L, 0L)), // output type
                            new BoltWrapper<>(new CounterBolt(counter_par_deg)))
                        .setParallelism(counter_par_deg);

            System.out.println("[main] Bolt Counter created.");

            DataStream<Tuple3<String, Long, Long>> sink =
                    counter_bolt
                        .transform(
                            Component.SINK, // operator name
                            TypeExtractor.getForObject(new Tuple3<>("", 0L, 0L)), // output type
                            new BoltWrapper<>(new ConsoleSink(sink_par_deg, gen_rate)))
                        .setParallelism(sink_par_deg);

            System.out.println("[main] Sink created.");

            System.out.println("[main] executing topology...");

            // print app info
            System.out.println("[SUMMARY] Executing WordCount with parameters:\n" +
                                "* file path: " + ((file_path != null) ? file_path : "random sentences") + "\n" +
                                "* source parallelism degree: " + source_par_deg + "\n" +
                                "* splitter parallelism degree: " + splitter_par_deg + "\n" +
                                "* counter parallelism degree: " + counter_par_deg + "\n" +
                                "* sink parallelism degree: " + sink_par_deg + "\n" +
                                "* rate: " + gen_rate + "\n" +
                                "Topology: source -> splitter -> counter -> sink");

            env.execute(topology_name);
        }
    }
}
