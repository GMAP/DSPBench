package org.dspbench.sink;

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
import java.util.HashMap;
import java.util.Map;


public abstract class BaseSink extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBolt.class);

    protected Formatter formatter;

    private final Map<String, Long> throughput = new HashMap<>(); //treemap put in order of key

    @Override
    public void initialize() {
        String formatterClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_FORMATTER), null);

        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(config, context);

//        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
//            this.lastTime = Instant.now().getEpochSecond();
//
//            try {
//                Path path = Paths.get(config.getString(Configuration.METRICS_OUTPUT), this.getClass().getSimpleName() + ".csv");
//                File file = path.toFile();
//                file.createNewFile();
//
//                writer = new BufferedWriter(new OutputStreamWriter(
//                        new FileOutputStream(file), "utf-8"));
//
//                writer.write("unix time" + "," + "op/s");
//                writer.newLine();
//
//            } catch (IOException ex) {
//                LOG.error("Error while creating file " + file, ex);
//            }
//        }
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

    @Override
    public void cleanup() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            super.cleanup();

            try {
                Path path = Paths.get(config.getString(Configuration.METRICS_OUTPUT), this.getClass().getSimpleName() + ".csv");
                File file = path.toFile();
                file.createNewFile();

                String eol = System.getProperty("line.separator");

                try (Writer writer = new FileWriter(file)) {
                    writer.append("UnixTime,op/s" + eol);
                    for (Map.Entry<String, Long> entry : throughput.entrySet()) {
                        writer.append(entry.getKey())
                                .append(',')
                                .append(entry.getValue() + "")
                                .append(eol);
                    }
                } catch (IOException ex) {
                    LOG.error("Error while writing the file " + file, ex);
                }
            } catch (IOException e) {
                LOG.error("Error while creating the file " + e.getMessage());
            }
        }
    }
}
