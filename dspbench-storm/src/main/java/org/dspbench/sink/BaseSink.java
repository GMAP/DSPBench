package org.dspbench.sink;

import org.apache.commons.io.FileUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.MutableLong;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.constants.BaseConstants;
import org.dspbench.sink.formatter.BasicFormatter;
import org.dspbench.sink.formatter.Formatter;
import org.dspbench.util.config.ClassLoaderUtils;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public abstract class BaseSink extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBolt.class);

    protected Formatter formatter;

    private final Map<String, Long> throughput = new HashMap<>();
    private final ArrayList<Long> latency = new ArrayList<Long>();

    @Override
    public void initialize() {
        String formatterClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_FORMATTER), null);

        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(config, context);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields("");
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    protected abstract Logger getLogger();

    public void calculateThroughput() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = Instant.now().getEpochSecond();

            Long ops = throughput.get(unixTime + "");
            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
    }

    public void calculateLatency(long UnixTimeInit) {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long UnixTimeEnd = 0;
            if (config.getString(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
                UnixTimeEnd = Instant.now().getEpochSecond();
            } else {
                UnixTimeEnd = Instant.now().toEpochMilli();
            }
            latency.add(UnixTimeEnd - UnixTimeInit);
        }
    }


    @Override
    public void cleanup() {
        super.cleanup();
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {

            try {
                Paths.get(config.getString(Configuration.METRICS_OUTPUT), "throughput").toFile().mkdirs();
                File file = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + ".csv").toFile();

                String eol = System.getProperty("line.separator");

                try (Writer writer = new FileWriter(file)) {
                    writer.append("UnixTime,op/s").append(eol);
                    for (Map.Entry<String, Long> entry : throughput.entrySet()) {
                        writer.append(entry.getKey())
                                .append(',')
                                .append(entry.getValue() + "")
                                .append(eol);
                    }

                    file = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "latency", this.getClass().getSimpleName() + ".csv").toFile();
                    FileUtils.writeLines(file, latency);

                } catch (IOException ex) {
                    LOG.error("Error while writing the file " + file, ex);
                }
            } catch (Exception e) {
                LOG.error("Error while creating the file " + e.getMessage());
            }
        }
    }
}
