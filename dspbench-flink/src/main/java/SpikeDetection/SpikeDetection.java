package SpikeDetection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.time.Instant;
import java.util.*;

public class SpikeDetection {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "192.168.20.167:9092";
        String topic = "spikeDetect";
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

        DataStream<String> data = env.readTextFile("/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-storm/data/sensors.dat");

        DataStream<Tuple3<String, Date, Double>> dataParse = data.map(new MapFunction<String, Tuple3<String, Date, Double>>() {
            @Override
            public Tuple3<String, Date, Double> map(String value) throws Exception {

                DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
                        .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
                        .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
                        .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
                        .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();

                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

                String[] temp = value.split("\\s+");

                if (temp.length != 8)
                    return null;

                String dateStr = String.format("%s %s", temp[0], temp[1]);
                DateTime date = null;

                try {
                    date = formatterMillis.parseDateTime(dateStr);
                } catch (IllegalArgumentException ex) {
                    try {
                        date = formatter.parseDateTime(dateStr);
                    } catch (IllegalArgumentException ex2) {
                        System.out.println("Error parsing record date/time field, input record: " + value + ", " + ex2);
                        return null;
                    }
                }

                try {
                    return new Tuple3<>(
                        temp[3],
                        date.toDate(),
                        Double.parseDouble(temp[4])
                    );
                } catch (NumberFormatException ex) {
                    System.out.println("Error parsing record numeric field, input record: " + value + ", " + ex);
                }

                return null;
            }
        });

        DataStream<Tuple4<String, Double, Double, String>> movingAvg = dataParse.flatMap(new MovingAverage());

        DataStream<Tuple4<String, Double, Double, String>> spikeDetect = movingAvg.flatMap(new SpikeDetect());

        spikeDetect.print().name("print-sink");

        env.execute("SpikeDetection");
    }

    public static final class MovingAverage implements FlatMapFunction<Tuple3<String, Date, Double>, Tuple4<String, Double, Double, String>>{
        // VARS
        private static int movingAverageWindow;
        private static Map<String, LinkedList<Double>> deviceIDtoStreamMap;
        private static Map<String, Double> deviceIDtoSumOfEvents;

        // INIT VARS
        static {
            movingAverageWindow = 1000;
            deviceIDtoStreamMap = new HashMap<>();
            deviceIDtoSumOfEvents = new HashMap<>();
        }

        // PROCESS HERE
        @Override
        public void flatMap(Tuple3<String, Date, Double> input, Collector<Tuple4<String, Double, Double, String>> out){
            if(input != null){
                String time = Instant.now().getEpochSecond() + "";
                String deviceID = input.getField(0);
                double nextDouble = input.getField(2);
                double movingAverageInstant = movingAverage(deviceID, nextDouble);

                out.collect(new Tuple4<String, Double, Double, String>(deviceID, movingAverageInstant, nextDouble, time));
            }
        }

        public double movingAverage(String deviceID, double nextDouble) {
            LinkedList<Double> valueList = new LinkedList<>();
            double sum = 0.0;

            if (deviceIDtoStreamMap.containsKey(deviceID)) {
                valueList = deviceIDtoStreamMap.get(deviceID);
                sum = deviceIDtoSumOfEvents.get(deviceID);
                if (valueList.size() > movingAverageWindow - 1) {
                    double valueToRemove = valueList.removeFirst();
                    sum -= valueToRemove;
                }
                valueList.addLast(nextDouble);
                sum += nextDouble;
                deviceIDtoSumOfEvents.put(deviceID, sum);
                deviceIDtoStreamMap.put(deviceID, valueList);
                return sum / valueList.size();
            } else {
                valueList.add(nextDouble);
                deviceIDtoStreamMap.put(deviceID, valueList);
                deviceIDtoSumOfEvents.put(deviceID, nextDouble);
                return nextDouble;
            }
        }
    }

    public static final class SpikeDetect implements FlatMapFunction<Tuple4<String, Double, Double, String>, Tuple4<String, Double, Double, String>>{
        // VARS
        private static double spikeThreshold;

        // INIT VARS
        static {
            spikeThreshold = 0.03d;
        }

        // PROCESS
        @Override
        public void flatMap(Tuple4<String, Double, Double, String> input, Collector<Tuple4<String, Double, Double, String>> out){
            String deviceID = input.getField(0);
            double movingAverageInstant = input.getField(1);
            double nextDouble = input.getField(2);

            if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
                out.collect(new Tuple4<String, Double, Double, String>(deviceID, movingAverageInstant, nextDouble, "spike detected"));
            }
        }
    }
}
