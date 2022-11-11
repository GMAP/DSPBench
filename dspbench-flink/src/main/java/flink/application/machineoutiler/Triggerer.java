package flink.application.machineoutiler;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Triggerer extends Metrics implements FlatMapFunction<Tuple6<String, Double, Long, Object, Double, String>, Tuple6<String, Double, Long, Boolean, Object, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(Triggerer.class);

    private static final double dupper = Math.sqrt(2);
    private static long previousTimestamp;
    private static List<Tuple> streamList;
    private static double minDataInstanceScore = Double.MAX_VALUE;
    private static double maxDataInstanceScore = 0;

    Configuration config;

    public Triggerer(Configuration config) {
        super.initialize(config);
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
    public void flatMap(Tuple6<String, Double, Long, Object, Double, String> input, Collector<Tuple6<String, Double, Long, Boolean, Object, String>> out) {
        super.initialize(config);
        getList();

        long timestamp = input.getField(2);

        if (timestamp > previousTimestamp) {
            // new batch of stream scores
            if (!streamList.isEmpty()) {
                List<Tuple> abnormalStreams = this.identifyAbnormalStreams();
                int medianIdx = (int) streamList.size() / 2;
                double minScore = abnormalStreams.get(0).getField(1);
                double medianScore = abnormalStreams.get(medianIdx).getField(1);

                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore = streamProfile.getField(1);
                    double curDataInstScore = streamProfile.getField(4);
                    boolean isAbnormal = false;

                    // current stream score deviates from the majority
                    if ((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        // check whether cur data instance score return to normal
                        if (curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }

                    if (isAbnormal) {
                        out.collect(new Tuple6<String, Double, Long, Boolean, Object,String>(streamProfile.getField(0), streamScore, streamProfile.getField(2), isAbnormal, streamProfile.getField(3), input.f5));
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
        super.calculateThroughput();
    }

    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<>();
        int medianIdx = (int)(streamList.size() / 2);
        BFPRT.bfprt(streamList, medianIdx);
        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}
