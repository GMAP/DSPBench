package MachineOutlier;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

public class MachineOutlier {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get things from .properties?

        String brokers = "192.168.20.167:9092";
        String topic = "machineOut";
        String groupId = UUID.randomUUID().toString();

        /* REDO THIS
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
        */

        DataStream<String> data = env.readTextFile("/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-storm/data/machine-usage.csv");

        // ID, timestamp, Observation(timestamp, id, cpu, memory)
        DataStream<Tuple3<String, Long, MachineMetadata>> dataParse = data.map(new MapFunction<String, Tuple3<String, Long, MachineMetadata>>() {
            @Override
            public Tuple3<String, Long, MachineMetadata> map(String value) throws Exception {
                String[] temp = value.split(",");
                return new Tuple3<>(
                        temp[0],
                        Long.parseLong(temp[1]) * 1000,
                        new MachineMetadata(Long.parseLong(temp[1]) * 1000, temp[0], Double.parseDouble(temp[2]), Double.parseDouble(temp[3]))
                );
            }
        });

        //dataParse.print().name("print-sink");
        /* WC Example
        DataStream<Tuple2<String, Integer>> scorer = text.flatMap(new WordCount.Tokenizer())
                .name("tokenizer")
                .keyBy(value -> value.f0)
                .sum(1)
                .name("counter");
        */

        //System.out.println(dataParse);

        //Pass what?
        DataStream<Tuple4<String, Double, Long, Object>> scorer = dataParse.flatMap(new Scorer());//.keyBy(value -> value.f0);

        DataStream<Tuple5<String, Double, Long, Object, Double>> anomaly = scorer.flatMap(new AnomalyScorer()).keyBy(value -> value.f0);

        DataStream<Tuple5<String, Double, Long, Boolean, Object>> triggerer = anomaly.flatMap(new Triggerer());

        triggerer.print().name("print-sink");

        env.execute("MachineOutlier");
    }

    public static final class Scorer implements FlatMapFunction<Tuple3<String, Long, MachineMetadata>, Tuple4<String, Double, Long, Object>> {

        private static long previousTimestamp;
        private static String dataTypeName;
        private static List<Object> observationList;
        private static DataInstanceScorer dataInstanceScorer;

        static {
            previousTimestamp = 0;
            dataTypeName = "machineMetadata";
            observationList = new ArrayList<>();
            dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
        }
        @Override
        public void flatMap(Tuple3<String, Long, MachineMetadata> input, Collector<Tuple4<String, Double, Long, Object>> out){

            long timestamp = input.getField(1);
            if (timestamp > previousTimestamp) {
                // a new batch of observation, calculate the scores of old batch and then emit
                if (!observationList.isEmpty()) {
                    List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                    for (ScorePackage scorePackage : scorePackageList) {
                        out.collect(new Tuple4<String, Double, Long, Object>(scorePackage.getId(), scorePackage.getScore(), previousTimestamp, scorePackage.getObj()));
                    }
                    observationList.clear();
                }

                previousTimestamp = timestamp;
            }

            observationList.add(input.getField(2));
            //observationList.add(input.getValueByField(MachineOutlierConstants.Field.OBSERVATION));
        }
    }

    public static final class AnomalyScorer implements FlatMapFunction<Tuple4<String, Double, Long, Object>, Tuple5<String, Double, Long, Object, Double>>{
        private static Map<String, Queue<Double>> slidingWindowMap;
        private static int windowLength;
        private static long previousTimestamp;

        static {
            slidingWindowMap = new HashMap<>();
            windowLength = 10;
            previousTimestamp = 0;
        }

        @Override
        public void flatMap(Tuple4<String, Double, Long, Object> input, Collector<Tuple5<String, Double, Long, Object, Double>> out) {
            long timestamp =  input.getField(2);
            String id = input.getField(0);
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

    public static final class Triggerer implements FlatMapFunction<Tuple5<String, Double, Long, Object, Double>, Tuple5<String, Double, Long, Boolean, Object>>{
        private static final double dupper = Math.sqrt(2);
        private static long previousTimestamp;
        private static List<Tuple> streamList;
        private static double minDataInstanceScore = Double.MAX_VALUE;
        private static double maxDataInstanceScore = 0;

        static{
            previousTimestamp = 0;
            streamList = new ArrayList<>();
        }

        @Override
        public void flatMap(Tuple5<String, Double, Long, Object, Double> input, Collector<Tuple5<String, Double, Long, Boolean, Object>> out) {
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
                            out.collect(new Tuple5<String, Double, Long, Boolean, Object>(streamProfile.getField(0), streamScore, streamProfile.getField(2), isAbnormal, streamProfile.getField(3)));
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
