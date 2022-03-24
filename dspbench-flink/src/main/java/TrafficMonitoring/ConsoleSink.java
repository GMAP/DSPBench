package TrafficMonitoring;

import Constants.TrafficMonitoringConstants.Field;
import Util.config.Configuration;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
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
 *  The sink is in charge of printing the results:
 *  the average speed on a certain road (identified by
 *  a roadID) and the number of data collected for that
 *  roadID.
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
        LOG.info("[Sink] Started ({} replicas).", par_deg);

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        tuple_latencies = new DescriptiveStatistics();

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        int roadID = tuple.getInteger(0);       // Field.ROAD_ID
        int avg_speed = tuple.getInteger(1);    // Field.AVG_SPEED
        int count = tuple.getInteger(2);        // Field.COUNT
        long timestamp = tuple.getLong(3);      // Field.TIMESTAMP

        LOG.debug("[Sink] tuple: roadID " + roadID +
                ", average_speed " + avg_speed +
                ", counter " + count +
                ", ts " + timestamp);

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
        outputFieldsDeclarer.declare(new Fields(Field.ROAD_ID, Field.AVG_SPEED, Field.COUNT, Field.TIMESTAMP));
    }
}
