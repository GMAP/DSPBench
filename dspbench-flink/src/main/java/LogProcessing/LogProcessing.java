package LogProcessing;

import LogProcessing.geoip.IPLocation;
import LogProcessing.geoip.IPLocationFactory;
import LogProcessing.geoip.Location;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogProcessing {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get things from .properties?

        String brokers = "192.168.20.167:9092";
        String topic = "logProcess";
        String groupId = UUID.randomUUID().toString();

        /*
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        */
        DataStream<String> data = env.readTextFile("/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-storm/data/http-server.log");

        DataStream<Tuple6<Object, Object, Long, Object, Object, Object>> dataParse = data.map(new MapFunction<String, Tuple6<Object, Object, Long, Object, Object, Object>>() {
            @Override
            public Tuple6<Object, Object, Long, Object, Object, Object> map(String value) throws Exception {

                Map<String, Object> entry = parseLine(value);

                if (entry == null) {
                    System.out.println("Unable to parse log: " + value);
                    return null;
                }

                long minute = DateUtils.getMinuteForTime((Date) entry.get("TIMESTAMP"));
                return new Tuple6<Object, Object, Long, Object, Object, Object>( entry.get("IP"), entry.get("TIMESTAMP"), minute, entry.get("REQUEST"), entry.get("RESPONSE"), entry.get("BYTE_SIZE"));
            }
        });

        DataStream<Tuple2<Long, Long>> volCount = dataParse.flatMap(new VolCounter());

        DataStream<Tuple2<Integer, Integer>> statusCount = dataParse.flatMap(new StatusCounter());

        DataStream<Tuple2<String, String>> geoFind = dataParse.flatMap(new GeoFinder());

        DataStream<Tuple4<String, Integer, String, Integer>> geoStats = geoFind.flatMap(new GeoStats());

        volCount.print().name("Volume-Counter");
        statusCount.print().name("Status-Counter");
        geoStats.print().name("Geo-Stats");

        env.execute("LogProcessing");
    }
    public static Map<String, Object> parseLine(String logLine) {
        Map<String, Object> entry = new HashMap<>();
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";


        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logLine);

        if (!matcher.matches() || 8 != matcher.groupCount()) {
            return null;
        }

        entry.put("IP", matcher.group(1));
        entry.put("TIMESTAMP", DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z").parseDateTime(matcher.group(4)).toDate());
        entry.put("REQUEST", matcher.group(5));
        entry.put("RESPONSE", Integer.parseInt(matcher.group(6)));

        if (matcher.group(7).equals("-"))
            entry.put("BYTE_SIZE", 0);
        else
            entry.put("BYTE_SIZE", Integer.parseInt(matcher.group(7)));

        return entry;
    }

    public static final class VolCounter implements FlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<Long, Long>> {
        // VARS
        private static CircularFifoBuffer buffer;
        private static Map<Long, MutableLong> counts;

        // INIT VARS
        static {
            int windowSize = 60;

            buffer = new CircularFifoBuffer(windowSize);
            counts = new HashMap<>(windowSize);
        }
        // PROCESS
        @Override
        public void flatMap(Tuple6<Object, Object, Long, Object, Object, Object> input, Collector<Tuple2<Long, Long>> out){
            long minute = input.getField(2);

            MutableLong count = counts.get(minute);

            if (count == null) {
                if (buffer.isFull()) {
                    long oldMinute = (Long) buffer.remove();
                    counts.remove(oldMinute);
                }

                count = new MutableLong(1);
                counts.put(minute, count);
                buffer.add(minute);
            } else {
                count.increment();
            }

            out.collect(new Tuple2<Long, Long>(minute, count.longValue()));
        }
    }

    public static final class StatusCounter implements FlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<Integer, Integer>> {
        // VARS
        private static Map<Integer, Integer> counts;

        // INIT VARS
        static {
            counts = new HashMap<>();
        }
        // PROCESS
        @Override
        public void flatMap(Tuple6<Object, Object, Long, Object, Object, Object> input, Collector<Tuple2<Integer, Integer>> out){
            int statusCode = input.getField(4);
            int count = 0;

            if (counts.containsKey(statusCode)) {
                count = counts.get(statusCode);
            }

            count++;
            counts.put(statusCode, count);

            out.collect(new Tuple2<Integer, Integer>(statusCode, count));
        }
    }

    public static final class GeoFinder implements FlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<String, String>> {
        // VARS
        private static IPLocation resolver;

        // INIT VARS
        static {
            String ipResolver = "geoip2";
            resolver = IPLocationFactory.create(ipResolver);
        }
        // PROCESS
        @Override
        public void flatMap(Tuple6<Object, Object, Long, Object, Object, Object> input, Collector<Tuple2<String, String>> out){
            String ip = input.getField(0);

            Location location = resolver.resolve(ip);

            if (location != null) {
                String city = location.getCity();
                String country = location.getCountryName();

                out.collect(new Tuple2<String, String>(country, city));
            }


        }
    }

    public static final class GeoStats implements FlatMapFunction<Tuple2<String, String>, Tuple4<String, Integer, String, Integer>> {
        // VARS
        private static Map<String, CountryStats> stats;

        // INIT VARS
        static {
            stats = new HashMap<>();
        }
        // PROCESS
        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple4<String, Integer, String, Integer>> out){
            String country = input.getField(0);
            String city    = input.getField(1);

            if (!stats.containsKey(country)) {
                stats.put(country, new CountryStats(country));
            }

            stats.get(country).cityFound(city);

            out.collect(new Tuple4<String, Integer, String, Integer>(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));
        }

        private class CountryStats {
            private int countryTotal = 0;

            private static final int COUNT_INDEX = 0;
            private static final int PERCENTAGE_INDEX = 1;

            private final String countryName;
            private final Map<String, List<Integer>> cityStats = new HashMap<>();

            public CountryStats(String countryName) {
                this.countryName = countryName;
            }

            public void cityFound(String cityName) {
                countryTotal++;

                if (cityStats.containsKey(cityName)) {
                    cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX) + 1 );
                } else {
                    List<Integer> list = new LinkedList<>();
                    list.add(1);
                    list.add(0);
                    cityStats.put(cityName, list);
                }

                double percent = (double)cityStats.get(cityName).get(COUNT_INDEX)/(double)countryTotal;
                cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
            }

            public int getCountryTotal() {
                return countryTotal;
            }

            public int getCityTotal(String cityName) {
                return cityStats.get(cityName).get(COUNT_INDEX);
            }

            @Override
            public String toString() {
                return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
                        + "Cities: " + cityStats.toString();
            }
        }
    }
}
