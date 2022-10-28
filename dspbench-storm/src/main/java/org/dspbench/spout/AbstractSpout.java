package org.dspbench.spout;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.constants.BaseConstants;
import org.dspbench.constants.BaseConstants.BaseStream;
import org.dspbench.hooks.SpoutMeterHook;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.util.config.Configuration.METRICS_ENABLED;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractSpout extends BaseRichSpout {
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;
    protected Map<String, Fields> fields;
    protected String configSubPrefix;

    private BlockingQueue<String> queue;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSpout.class);
    private final Map<String, Long> throughput = new HashMap<>();

    public AbstractSpout() {
        fields = new HashMap<>();
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.config = Configuration.fromMap(conf);
        this.collector = collector;
        this.context = context;

        if (config.getBoolean(METRICS_ENABLED, false)) {
            File pathTrh = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "throughput").toFile();
            pathTrh.mkdirs();

            queue = new ArrayBlockingQueue<>(50);

            this.file = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
        }
        initialize();
    }

    protected String getConfigKey(String template, boolean useSubPrefix) {
        if (useSubPrefix && StringUtils.isNotEmpty(configSubPrefix)) {
            return String.format(template, String.format("%s.%s", configPrefix, configSubPrefix));
        }

        return String.format(template, configPrefix);
    }

    protected String getConfigKey(String template) {
        return getConfigKey(template, false);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    public void setConfigSubPrefix(String configSubPrefix) {
        this.configSubPrefix = configSubPrefix;
    }

    protected abstract void initialize();

    public void calculateThroughput() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }

            Long ops = throughput.get(unixTime + "");
            if (ops == null) {
                for (Map.Entry<String, Long> entry : this.throughput.entrySet()) {
                    this.queue.add(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                }
                throughput.clear();
                if (queue.size() >= 10) {
                    SaveMetrics();
                }
            }

            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
    }


    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    LOG.error("Error while writing the file " + file, ex);
                }
            } catch (Exception e) {
                LOG.error("Error while creating the file " + e.getMessage());
            }
        }).start();
    }
}
