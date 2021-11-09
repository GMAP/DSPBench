package WordCount;

import Constants.WordCountConstants.Conf;
import Constants.WordCountConstants.Field;
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
import java.util.Random;
import java.util.Scanner;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  The spout is in charge of reading the input data file containing
 *  words, parsing it and generating a stream of lines toward the
 *  SplitterBolt. In alternative it generates a stream of random
 *  sentences.
 */
public class FileParserSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(FileParserSpout.class);

    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;

    private String type;
    private String file_path;
    private int rate;

    private long t_start;
    private long generated;
    private long emitted;
    private long nt_execution;
    private long nt_end;
    private int par_deg;

    private ArrayList<String> lines;    // state of the spout (contains parsed data)
    private long bytes;                 // accumulates the amount of bytes emitted

    /**
     * Constructor: it expects the source type, the file path, the generation rate and the parallelism degree.
     * @param source_type can be file or generator
     * @param file path to the input data file (null if the source type is generator)
     * @param gen_rate if the argument value is -1 then the spout generates tuples at
     *                 the maximum rate possible (measure the bandwidth under this assumption);
     *                 if the argument value is different from -1 then the spout generates
     *                 tuples at the rate given by this parameter (measure the latency given
     *                 this generation rate)
     * @param p_deg source parallelism degree
     */
    FileParserSpout(String source_type, String file, int gen_rate, int p_deg) {
        type = source_type;
        file_path = file;
        rate = gen_rate;            // number of tuples per second
        par_deg = p_deg;            // spout parallelism degree
        generated = 0;              // total number of generated tuples
        emitted = 0;                // number of emitted tuples
        nt_execution = 0;           // number of executions of nextTuple() method

        lines = new ArrayList<>();
        bytes = 0;                  // total number of bytes emitted
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        LOG.info("[Source] Started ({} replicas with rate {}).", par_deg, rate);

        t_start = System.nanoTime(); // spout start time in nanoseconds

        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;

        // save tuples as a state
        parseDataset();
    }

    /**
     * The method is called in an infinite loop by design, this means that the
     * stream is continuously generated from the data source file.
     */
    @Override
    public void nextTuple() {
        int interval = 1000000000; // one second (nanoseconds)
        long t_init = System.nanoTime();

        for (int i = 0; i < lines.size(); i++) {
            if (rate == -1) {       // at the maximum possible rate
                collector.emit(new Values(lines.get(i), System.nanoTime()));
                bytes += lines.get(i).getBytes().length;
                generated++;
            } else {                // at the given rate
                long t_now = System.nanoTime();
                if (emitted >= rate) {
                    if (t_now - t_init <= interval)
                        active_delay(interval - (t_now - t_init));
                    emitted = 0;
                    t_init = System.nanoTime();
                }
                collector.emit(new Values(lines.get(i), System.nanoTime()));
                bytes += lines.get(i).getBytes().length;
                emitted++;
                generated++;
                active_delay((double) interval / rate);
            }
        }
        nt_execution++;
        nt_end = System.nanoTime();
    }

    @Override
    public void close() {
        long t_elapsed = (nt_end - t_start) / 1000000;  // elapsed time in milliseconds

        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);

        System.out.println("[Source] execution time: " + t_elapsed +
                            " ms, generations: " + nt_execution +
                            ", generated: " + generated + " (lines) " + (bytes / 1048576) +
                            " (MB), bandwidth: " + generated / (t_elapsed / 1000) +
                            " (lines/s) " + formatted_mbs + " (MB/s).");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.LINE, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------

    private void parseDataset() {
        if (type.equals(Conf.FILE_SOURCE)) {
            try {
                Scanner scan = new Scanner(new File(file_path));
                while (scan.hasNextLine()) {
                    lines.add(scan.nextLine());
                    generated++;
                }
                scan.close();
                LOG.info("[Source] parsed dataset: " + generated + " tuples");
                generated = 0;
            } catch (FileNotFoundException | NullPointerException e) {
                LOG.error("The file {} does not exists", file_path);
                throw new RuntimeException("The file '" + file_path + "' does not exists");
            }
        } else {
            String[] sentences = new String[]{
                    "the cow jumped over the moon", "an apple a day keeps the doctor away",
                    "four score and seven years ago", "snow white and the seven dwarfs",
                    "i am at two with nature", "the truculent guitar can't advise the quarter",
                    "it was then the excellent coach met the purring resource",
                    "the unfinished guest includes into the husky mate"
            };
            Random rand = new Random();
            for (int i = 0; i < 50000; i++) {
                lines.add(sentences[rand.nextInt(sentences.length)]);
                generated++;
            }
            LOG.info("[Source] parsed dataset: " + generated + " tuples");
            generated = 0;
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
