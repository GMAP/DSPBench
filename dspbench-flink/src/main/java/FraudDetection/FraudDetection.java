package FraudDetection;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.UUID;
import org.apache.flink.util.Collector;

public class FraudDetection {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "192.168.20.167:9092";
        String topic = "fraudDetect";
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

        DataStream<String> data = env.readTextFile("/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-storm/data/credit-card.dat");

        DataStream<Tuple2<String, String>> dataParse = data.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] temp = value.split(",", 2);
                return new Tuple2<>(
                        temp[0],
                        temp[1]
                );
            }
        });

        DataStream<Tuple3<String, Double, String>> fraudPredict = dataParse.flatMap(new FraudPredictor());

        fraudPredict.print().name("print-sink");

        env.execute("FraudDetection");

    }

    public static final class FraudPredictor implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, Double, String>>{
        // VARS
        private static ModelBasedPredictor predictor;
        static String strategy;

        // INIT VARS
        static {
            strategy = "mm";

            if (strategy.equals("mm")) {
                predictor = new MarkovModelPredictor();
            }
        }

        // PROCESS
        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple3<String, Double, String>> out){
            String entityID = input.getField(0);
            String record = input.getField(1);
            Prediction p = predictor.execute(entityID, record);

            // send outliers
            if (p.isOutlier()) {
                out.collect(new Tuple3<>(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
            }
        }
    }
}
