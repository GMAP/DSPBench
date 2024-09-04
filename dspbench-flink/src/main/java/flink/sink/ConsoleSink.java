package flink.sink;

import flink.application.YSB.Aggregate_Event;
import flink.application.voipstream.CallDetailRecord;
import flink.constants.*;
import flink.tools.Rankings;
import flink.util.Metrics;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.BooleanValue;
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

    Metrics metrics = new Metrics();

    public void initialize(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
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
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(WordCountConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamTM(DataStream<Tuple4<Date, Integer, Integer, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple4<Date, Integer, Integer, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<Date, Integer, Integer, Integer> value, Context context)
                    throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(TrafficMonitoringConstants.Conf.SINK_THREADS, 1));
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
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<String, Double, Double, String> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(SpikeDetectionConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSA(DataStream<Tuple5<String, String, Date, String, Double>> input) {
        input.addSink(new RichSinkFunction<Tuple5<String, String, Date, String, Double>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple5<String, String, Date, String, Double> value, Context context)
                    throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(SentimentAnalysisConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamMO(DataStream<Tuple5<String, Double, Long, Boolean, Object>> input) {
        input.addSink(new RichSinkFunction<Tuple5<String, Double, Long, Boolean, Object>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple5<String, Double, Long, Boolean, Object> value, Context context)
                    throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(MachineOutlierConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamFD(DataStream<Tuple3<String, Double, String>> input) {
        input.addSink(new RichSinkFunction<Tuple3<String, Double, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple3<String, Double, String> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(FraudDetectionConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSGOutlier(DataStream<Tuple4<Long, Long, String, Double>> input) {
        input.addSink(new RichSinkFunction<Tuple4<Long, Long, String, Double>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<Long, Long, String, Double> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(SmartGridConstants.Conf.OUTLIER_SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSGHouse(DataStream<Tuple3<Long, String, Double>> input) {
        input.addSink(new RichSinkFunction<Tuple3<Long, String, Double>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple3<Long, String, Double> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(SmartGridConstants.Conf.PREDICTION_SINK_THREADS, 1));
    }

    @Override
    public void sinkStreamSGPlug(DataStream<Tuple5<Long, String, String, String, Double>> input) {
        input.addSink(new RichSinkFunction<Tuple5<Long, String, String, String, Double>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple5<Long, String, String, String, Double> value, Context context)
                    throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(SmartGridConstants.Conf.PREDICTION_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPVol(DataStream<Tuple2<Long, Long>> input) {
        input.addSink(new RichSinkFunction<Tuple2<Long, Long>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(LogProcessingConstants.Conf.VOLUME_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(LogProcessingConstants.Conf.STATUS_SINK_THREADS, 1));
    }

    @Override
    public void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple4<String, Integer, String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<String, Integer, String, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(LogProcessingConstants.Conf.GEO_SINK_THREADS, 1));
    }

    @Override
    public void createSinkCAStatus(DataStream<Tuple2<Integer, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(ClickAnalyticsConstants.Conf.TOTAL_SINK_THREADS, 1));
    }

    @Override
    public void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple4<String, Integer, String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<String, Integer, String, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(ClickAnalyticsConstants.Conf.GEO_SINK_THREADS, 1));
    }

    @Override
    public void createSinkAA(DataStream<Tuple6<String, String, Double, Long, Long, Integer>> input) {
        input.addSink(new RichSinkFunction<Tuple6<String, String, Double, Long, Long, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple6<String, String, Double, Long, Long, Integer> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(AdAnalyticsConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkBI(DataStream<Tuple4<String, Double, Integer, Double>> input) {
        input.addSink(new RichSinkFunction<Tuple4<String, Double, Integer, Double>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<String, Double, Integer, Double> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(BargainIndexConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkRL(DataStream<Tuple2<String, String[]>> input) {
        input.addSink(new RichSinkFunction<Tuple2<String, String[]>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple2<String, String[]> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(ReinforcementLearnerConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkSF(DataStream<Tuple3<String,Float,Boolean>> input) {
        input.addSink(new RichSinkFunction<Tuple3<String,Float,Boolean>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple3<String,Float,Boolean> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(SpamFilterConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkTT(DataStream<Tuple1<Rankings>> input) {
        input.addSink(new RichSinkFunction<Tuple1<Rankings>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple1<Rankings> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(TrendingTopicsConstants.Conf.SINK_THREADS, 1));
    }

    @Override
    public void createSinkVS(DataStream<Tuple4<String, Long, Double, CallDetailRecord>> input) {
        input.addSink(new RichSinkFunction<Tuple4<String, Long, Double, CallDetailRecord>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Tuple4<String, Long, Double, CallDetailRecord> value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(VoIPStreamConstants.Conf.SINK_THREADS, 1));
    }


    @Override
    public void sinkStreamYSB(SingleOutputStreamOperator<Aggregate_Event> input) {
        input.addSink(new RichSinkFunction<Aggregate_Event>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                metrics.SaveMetrics();
            }

            @Override
            public void invoke(Aggregate_Event value, Context context) throws Exception {
                super.invoke(value, context);
                // System.out.println(value);
                calculate("0");
            }
        }).setParallelism(config.getInteger(YSBConstants.Conf.SINK_THREADS, 1));
    }

    public void calculate(String initTime) {
        //super.initialize(config);
        //super.incReceived();
        metrics.receiveThroughput();
        // super.calculateLatency(Long.parseLong(initTime));
        // super.calculateThroughput();
    }

    public void calculate(String initTime, String sinkName) {
        //super.initialize(config, sinkName);
        //super.incReceived();
        metrics.receiveThroughput();
        // super.calculateThroughput();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
