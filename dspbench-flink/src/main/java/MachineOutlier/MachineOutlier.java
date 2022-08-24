package MachineOutlier;

import WordCount.WordCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.operators.shipping.OutputCollector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.*;

public class MachineOutlier {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get things from .properties?

        String brokers = "192.168.20.167:9092";
        String topic = "machineoutlier";
        String groupId = UUID.randomUUID().toString();

        KafkaSource<Tuple3<String, String, String>> source = KafkaSource.<Tuple3<String, String, String>>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaRecordDeserializationSchema<Tuple3<String,String, String>>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple3<String, String, String>> out) throws IOException {

                    }

                    @Override
                    public TypeInformation<Tuple3<String, String, String>> getProducedType() {
                        return null;
                    }
                })
                .build();

        // Need to know the input order
        DataStream< Tuple3<String, String, String>> data = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        /* WC Example
        DataStream<Tuple2<String, Integer>> scorer = text.flatMap(new WordCount.Tokenizer())
                .name("tokenizer")
                .keyBy(value -> value.f0)
                .sum(1)
                .name("counter");
        */

        //Pass what?
        DataStream<Tuple4<String, Double, Long, Object>> scorer = data.flatMap(new Scorer());

        DataStream<Tuple5<String, Double, Long, Object, Double>> anomaly = scorer.flatMap(new AnomalyScorer()).keyBy(value -> value.f0);

        DataStream<Tuple5<String, Double, Long, Boolean, String>> triggerer = anomaly.flatMap(new Triggerer());

        triggerer.print().name("print-sink");

        env.execute("MachineOutlier");
    }

    public static final class Scorer implements FlatMapFunction<Tuple3<String, String, String>, Tuple4<String, Double, Long, Object>> {

        private long previousTimestamp = 0;
        private String dataTypeName = "machineMetadata";
        private List<Object> observationList = new ArrayList<>();
        private DataInstanceScorer dataInstanceScorer;

        @Override
        public void flatMap(Tuple3<String, String, String> input, Collector<Tuple4<String, Double, Long, Object>> out){

            long timestamp = input.getField(1);

            if (timestamp > previousTimestamp) {
                // a new batch of observation, calculate the scores of old batch and then emit
                if (!observationList.isEmpty()) {
                    List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                    for (ScorePackage scorePackage : scorePackageList) {
                        //Pass to next!
                        out.collect(new Tuple4<String, Double, Long, Object>(scorePackage.getId(), scorePackage.getScore(), previousTimestamp, scorePackage.getObj()));
                    }
                    observationList.clear();
                }

                previousTimestamp = timestamp;
            }

            observationList.add(input.getField(2));
            //observationList.add(input.getValueByField(MachineOutlierConstants.Field.OBSERVATION));

            //Do I need to acknowledge?
            //collector.ack(input);
        }

    }

    public static final class AnomalyScorer implements FlatMapFunction<Tuple4<String, Double, Long, Object>, Tuple5<String, Double, Long, Object, Double>>{
        private Map<String, Queue<Double>> slidingWindowMap = new HashMap<>();
        private int windowLength = 10;
        private long previousTimestamp = 0;

        @Override
        public void flatMap(Tuple4<String, Double, Long, Object> input, Collector<Tuple5<String, Double, Long, Object, Double>> out) {
            long timestamp =  input.getField(2);
            String id = input.getField(0); //MACHINE ID
            double dataInstanceAnomalyScore = input.getField(1);

            Queue<Double> slidingWindow = slidingWindowMap.get(id);
            if (slidingWindow == null) {
                slidingWindow = new LinkedList<>();
            }

            // update sliding window
            slidingWindow.add(dataInstanceAnomalyScore);
            if (slidingWindow.size() > this.windowLength) {
                slidingWindow.poll();
            }
            slidingWindowMap.put(id, slidingWindow);

            double sumScore = 0.0;
            for (double score : slidingWindow) {
                sumScore += score;
            }

            out.collect(new Tuple5<String, Double, Long, Object, Double>(id, sumScore, timestamp, input.getField(3), dataInstanceAnomalyScore));
            //collector.ack(input);
        }
    }

    public static final class Triggerer implements FlatMapFunction<Tuple5<String, Double, Long, Object, Double>, Tuple5<String, Double, Long, Boolean, String>>{
        private static final double dupper = Math.sqrt(2);
        private long previousTimestamp;
        private List<Tuple> streamList;
        private double minDataInstanceScore = Double.MAX_VALUE;
        private double maxDataInstanceScore = 0;

        @Override
        public void flatMap(Tuple5<String, Double, Long, Object, Double> input, Collector<Tuple5<String, Double, Long, Boolean, String>> out) {
            long timestamp = input.getField(0);

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
                            out.collect(new Tuple5<String, Double, Long, Boolean, String>(streamProfile.getField(0), streamScore, streamProfile.getField(2), isAbnormal, streamProfile.getField(3)));
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
            //collector.ack(input);
        }

        private List<Tuple> identifyAbnormalStreams() {
            List<Tuple> abnormalStreamList = new ArrayList<>();
            int medianIdx = (int)(streamList.size() / 2);
            BFPRT.bfprt(streamList, medianIdx);
            abnormalStreamList.addAll(streamList);
            return abnormalStreamList;
        }
    }
}
