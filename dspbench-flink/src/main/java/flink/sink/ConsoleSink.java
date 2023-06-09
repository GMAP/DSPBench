package flink.sink;

import flink.constants.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;

/**
 *
 */
public class ConsoleSink extends BaseSink implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    Configuration config;

    public void initialize(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void sinkStreamWC(DataStream<Tuple2<String, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                //System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(WordCountConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamTM(DataStream<Tuple5<Date, Integer, Integer, Integer, String>> input) {
        input.addSink(new RichSinkFunction<Tuple5<Date, Integer, Integer, Integer, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple5<Date, Integer, Integer, Integer, String>value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f4);
            }
        });
    }

    @Override
    public void sinkStreamSD(DataStream<Tuple4<String, Double, Double, String>> input) {
        input.addSink(new RichSinkFunction<Tuple4<String, Double, Double, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple4<String, Double, Double, String>value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate("0");
            }
        });
    }
    @Override
    public void sinkStreamSA(DataStream<Tuple6<String, String, Date, String, Double, String>> input) {
        input.addSink(new RichSinkFunction<Tuple6<String, String, Date, String, Double, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple6<String, String, Date, String, Double, String> value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f5);
            }
        });
    }

    @Override
    public void sinkStreamMO(DataStream<Tuple6<String, Double, Long, Boolean, Object, String>> input) {
        input.addSink(new RichSinkFunction<Tuple6<String, Double, Long, Boolean, Object, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple6<String, Double, Long, Boolean, Object, String> value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f5);
            }
        });
    }

    @Override
    public void sinkStreamFD(DataStream<Tuple4<String, Double, String,String>> input) {
        input.addSink(new RichSinkFunction<Tuple4<String, Double, String,String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple4<String, Double, String,String> value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f3);
            }
        });
    }

    @Override
    public void sinkStreamSGOutlier(DataStream<Tuple5<Long, Long, String, Double, String>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple5<Long, Long, String, Double, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple5<Long, Long, String, Double, String> value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f4, sinkName);
            }
        });
    }

    @Override
    public void sinkStreamSGHouse(DataStream<Tuple4<Long,String, Double, String>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple4<Long,String, Double, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple4<Long,String, Double, String> value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f3, sinkName);
            }
        });
    }

    @Override
    public void sinkStreamSGPlug(DataStream<Tuple6<Long,String, String, String, Double, String>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple6<Long,String, String, String, Double, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple6<Long,String, String, String, Double, String> value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                calculate(value.f5, sinkName);
            }
        });
    }

    @Override
    public void createSinkLPVol(DataStream<Tuple2<Long, Long>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple2<Long, Long>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
                super.invoke(value, context);
                //System.out.println(value);
                calculate("0", sinkName);
            }
        });
    }

    @Override
    public void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                //System.out.println(value);
                calculate("0", sinkName);
            }
        }).setParallelism(config.getInteger(LogProcessingConstants.Conf.STATUS_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple4<String, Integer, String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple4<String, Integer, String, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                //System.out.println(value);
                calculate("0", sinkName);
            }
        }).setParallelism(config.getInteger(LogProcessingConstants.Conf.GEO_SINK_THREADS, 1));
    }

    @Override
    public void createSinkCAStatus(DataStream<Tuple2<Integer, Integer>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                //System.out.println(value);
                calculate("0", sinkName);
            }
        }).setParallelism(config.getInteger(ClickAnalyticsConstants.Conf.STATUS_SINK_THREADS, 1));
    }

    @Override
    public void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> input, String sinkName) {
        input.addSink(new RichSinkFunction<Tuple4<String, Integer, String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void invoke(Tuple4<String, Integer, String, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                //System.out.println(value);
                calculate("0", sinkName);
            }
        }).setParallelism(config.getInteger(ClickAnalyticsConstants.Conf.GEO_SINK_THREADS, 1));
    }

    public void calculate(String initTime){
        super.initialize(config);
        super.incReceived();
        //super.calculateLatency(Long.parseLong(initTime));
        //super.calculateThroughput();
    }

    public void calculate(String initTime, String sinkName){
        super.initialize(config, sinkName);
        super.incReceived();
        //super.calculateThroughput();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
