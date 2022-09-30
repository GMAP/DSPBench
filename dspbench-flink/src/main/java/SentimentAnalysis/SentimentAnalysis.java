package SentimentAnalysis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;

import java.time.Instant;
import java.util.*;

public class SentimentAnalysis {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "192.168.20.167:9092";
        String topic = "SentimentAnalis";
        String groupId = UUID.randomUUID().toString();

        DataStream<String> data = env.readTextFile("/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-storm/data/tweetstream.jsonl");

        DataStream<Tuple3<String, String, Date>> dataParse = data.map(new MapFunction<String, Tuple3<String, String, Date>>() {
            @Override
            public Tuple3<String, String, Date> map(String value) throws Exception {
                JsonParser jsonParser = new JsonParser();
                DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

                Tuple1<JSONObject> parsed = jsonParser.parse(value);

                JSONObject tweet = (JSONObject) parsed.getField(0);

                if (tweet.containsKey("data")) {
                    tweet = (JSONObject) tweet.get("data");
                }

                if (!tweet.containsKey("id") || !tweet.containsKey("text") || !tweet.containsKey("created_at"))
                    return null;

                String id = (String) tweet.get("id");
                String text = (String) tweet.get("text");
                DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

                return new Tuple3<String, String, Date>(id, text, timestamp.toDate());
            }
        });

        DataStream<Tuple2<Tuple3<String, String, Date>, Tuple6<String, String, Date, String, Double, String>>> calculate = dataParse.flatMap(new SentimentCalculator());//.keyBy(value -> value.f0);

        calculate.print().name("print-sink");

        env.execute("SentimentAnalysis");
    }

    public static final class SentimentCalculator implements FlatMapFunction<Tuple3<String, String, Date>, Tuple2<Tuple3<String, String, Date>, Tuple6<String, String, Date, String, Double, String>>> {

        // VARS
        private static DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH);
        private static SentimentClassifier classifier;

        static {
            // INIT VARS
            String classifierType = "basic"; // ESOTERIC
            classifier = SentimentClassifierFactory.create(classifierType);
        }

        @Override
        public void flatMap(Tuple3<String, String, Date> input, Collector<Tuple2<Tuple3<String, String, Date>, Tuple6<String, String, Date, String, Double, String>>> out){
            // DO PROCESSING
            String time = Instant.now().getEpochSecond() + "";
            String tweetId = input.getField(0);
            String text = input.getField(1);
            Date timestamp = input.getField(2);

            SentimentResult result = classifier.classify(text);

            out.collect(new Tuple2<>(input, new Tuple6<String, String, Date, String, Double, String>(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore(), time)));
        }
    }
}
