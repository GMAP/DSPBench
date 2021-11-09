package SpikeDetection;

import Constants.SpikeDetectionConstants.Field;
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
    private long processed;
    private int par_deg;
    private int gen_rate;

    private DescriptiveStatistics tuple_latencies;

    ConsoleSink(int p_deg, int g_rate) {
        par_deg = p_deg;         // sink parallelism degree
        gen_rate = g_rate;       // generation rate of the source (spout)
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        LOG.info("[Sink] started ({} replicas)", par_deg);

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        tuple_latencies = new DescriptiveStatistics();

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String deviceID = tuple.getString(0);               // Field.DEVICE_ID
        double moving_avg_instant = tuple.getDouble(1);     // Field.MOVING_AVG
        double next_property_value = tuple.getDouble(2);    // Field.VALUE
        long timestamp = tuple.getLong(3);                  // Field.TIMESTAMP

        LOG.debug("[Sink] outlier: deviceID " + deviceID +
                ", incremental_average " + moving_avg_instant +
                ", next_value " + next_property_value);

        // evaluate latency
        long now = System.nanoTime();
        double tuple_latency = (double)(now - timestamp) / 1000000.0; // tuple latency in ms
        tuple_latencies.addValue(tuple_latency);

        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        if (processed == 0) {
            LOG.info("[Sink] processed tuples: " + processed);
        } else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

            // bandwidth summary
            System.out.println("[Sink] processed tuples: " + processed +
                                ", bandwidth: " +  processed / (t_elapsed / 1000) +
                                " tuples/s.");

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
        outputFieldsDeclarer.declare(new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE, Field.TIMESTAMP));
    }
}

