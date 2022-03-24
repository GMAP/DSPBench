package WordCount;

import Constants.WordCountConstants.Field;
import Util.config.Configuration;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 *  @author  Alessandra Fais
 *  @version Nov 2019
 *
 *  Sink node that receives and prints the results.
 */
public class ConsoleSink extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

    private long t_start;
    private long t_end;
    private int par_deg;
    private int gen_rate;
    private long bytes;
    private long words;

    private DescriptiveStatistics tuple_latencies;

    ConsoleSink(int p_deg, int g_rate) {
        par_deg = p_deg;         // sink parallelism degree
        gen_rate = g_rate;       // generation rate of the source (spout)
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        LOG.info("[Sink] started ({} replicas)", par_deg);

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        bytes = 0;                   // total number of processed bytes
        words = 0;                   // total number of processed words
        tuple_latencies = new DescriptiveStatistics();

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);       // Field.WORD
        long count = tuple.getLong(1);          // Field.COUNT
        long timestamp = tuple.getLong(2);      // Field.TIMESTAMP

        LOG.debug("[Sink] Received `" +
                word + "` occurred " +
                count + " times since now.");

        // evaluate latency
        long now = System.nanoTime();
        double tuple_latency = (double)(now - timestamp) / 1000000.0; // tuple latency in ms
        tuple_latencies.addValue(tuple_latency);

        bytes += word.getBytes().length;
        words++;
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        if (bytes == 0) {
            System.out.println("[Sink] processed: " + bytes + " (bytes) " + words + " (words)");
        } else {
            // evaluate bandwidth and latency
            long t_elapsed = (t_end - t_start) / 1000000;       // elapsed time in ms

            double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
            String formatted_mbs = String.format("%.5f", mbs);

            // bandwidth summary
            System.out.println("[Sink] processed: " +
                                words + " (words) " +
                                (bytes / 1048576) + " (MB), " +
                                "bandwidth: " +
                                words / (t_elapsed / 1000) + " (words/s) " +
                                formatted_mbs + " (MB/s) " +
                                bytes / (t_elapsed / 1000) + " (bytes/s).");

            // latency summary
            System.out.println("[Sink] latency (ms): " +
                                tuple_latencies.getMean() + " (mean) " +
                                tuple_latencies.getMin() + " (min) " +
                                tuple_latencies.getPercentile(5) + " (5th) " +
                                tuple_latencies.getPercentile(25) + " (25th) " +
                                tuple_latencies.getPercentile(50) + " (50th) " +
                                tuple_latencies.getPercentile(75) + " (75th) " +
                                tuple_latencies.getPercentile(95) + " (95th) " +
                                tuple_latencies.getMax() + " (max).");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.WORD, Field.COUNT, Field.TIMESTAMP));
    }
}
