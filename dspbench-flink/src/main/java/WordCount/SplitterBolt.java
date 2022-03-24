package WordCount;

import Constants.WordCountConstants.Field;
import Util.config.Configuration;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 *  @author  Alessandra Fais
 *  @version Nov 2019
 *
 *  Splits all the received lines into words.
 */
public class SplitterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitterBolt.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

    private long t_start;
    private long t_end;
    private int par_deg;
    private long bytes;
    private long line_count;
    private long word_count;

    SplitterBolt(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        LOG.info("[Splitter] Started ({} replicas).", par_deg);

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        bytes = 0;                   // total number of processed bytes
        line_count = 0;              // total number of processed input tuples (lines)
        word_count = 0;              // total number of emitted tuples (words)

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getString(0);       // Field.LINE
        long timestamp = tuple.getLong(1);      // Field.TIMESTAMP

        if (line != null) {
            LOG.debug("[Splitter] Received line `" + line + "`");
            bytes += line.getBytes().length;
            line_count++;

            String[] words = line.split("\\W");
            for (String word : words) {
                if (!StringUtils.isBlank(word)) {

                    // emit unanchored tuple
                    collector.emit(new Values(word, timestamp));

                    LOG.debug("[SplitterBolt] Sending `" + word + "`");
                    word_count++;
                }
            }
        }

        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);

        System.out.println("[Splitter] execution time: " + t_elapsed + " ms, " +
                            "processed: " + line_count + " (lines) "
                            + word_count + " (words) "
                            + (bytes / 1048576) + " (MB), " +
                            "bandwidth: " + word_count / (t_elapsed / 1000) + " (words/s) "
                            + formatted_mbs + " (MB/s) "
                            + bytes / (t_elapsed / 1000) + " (bytes/s)");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.WORD, Field.TIMESTAMP));
    }
}

