package flink.application.machineoutiler;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Triggerer extends RichFlatMapFunction<Tuple5<String, Double, Long, Object, Double>, Tuple5<String, Double, Long, Boolean, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(Triggerer.class);

    private static final double dupper = Math.sqrt(2);
    private static long previousTimestamp;
    private static List<Tuple> streamList;
    private static double minDataInstanceScore = Double.MAX_VALUE;
    private static double maxDataInstanceScore = 0;

    Configuration config;
    Metrics metrics = new Metrics();

    public Triggerer(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
        previousTimestamp = 0;
    }

    private List<Tuple> getList() {
        if (streamList == null) {
            streamList = new ArrayList<>();
        }

        return streamList;
    }

    @Override
    public void flatMap(Tuple5<String, Double, Long, Object, Double> input,
            Collector<Tuple5<String, Double, Long, Boolean, Object>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        getList();

        long timestamp = input.getField(2);

        if (timestamp > previousTimestamp) {
            // new batch of stream scores
            if (!streamList.isEmpty()) {
                List<Tuple> abnormalStreams = identifyAbnormalStreams();
                int medianIdx = streamList.size() / 2;
                double minScore = abnormalStreams.get(0).getField(1);
                double medianScore = abnormalStreams.get(medianIdx).getField(1);

                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore = streamProfile.getField(1);
                    double curDataInstScore = streamProfile.getField(4);
                    boolean isAbnormal = false;

                    /*LOG.info("streamProfile " + Integer.toString(i) + " : " + streamProfile.toString());
                    LOG.info("curDataInstScore " + Integer.toString(i) + " : " + Double.toString(curDataInstScore));
                    LOG.info("streamScore " + Integer.toString(i) + " : " + Double.toString(streamScore));
                    LOG.info("medianScore " + Integer.toString(i) + " : " + Double.toString(medianScore));
                    LOG.info("minScore " + Integer.toString(i) + " : " + Double.toString(minScore));*/

                    // current stream score deviates from the majority
                    if ((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        
                        // check whether cur data instance score return to normal
                        if (curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }

                    if (isAbnormal) {
                        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                            metrics.emittedThroughput();
                        }
                        out.collect(new Tuple5<String, Double, Long, Boolean, Object>(streamProfile.getField(0),
                                streamScore, streamProfile.getField(2), isAbnormal, streamProfile.getField(3)));
                    }
                }

                streamList.clear();
                minDataInstanceScore = Double.MAX_VALUE;
                maxDataInstanceScore = 0;
            }

            previousTimestamp = timestamp;
        }

        double dataInstScore = input.getField(4);
        if (dataInstScore > maxDataInstanceScore) {
            maxDataInstanceScore = dataInstScore;
        }

        if (dataInstScore < minDataInstanceScore) {
            minDataInstanceScore = dataInstScore;
        }

        streamList.add(input);
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<>();
        int medianIdx = streamList.size() / 2;
        BFPRT.bfprt(streamList, medianIdx);
        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}
