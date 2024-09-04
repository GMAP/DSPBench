package flink.sink;

import flink.application.YSB.Aggregate_Event;
import flink.application.voipstream.CallDetailRecord;
import flink.constants.BaseConstants;
import flink.tools.Rankings;
import flink.util.Metrics;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.BooleanValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public abstract class BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    Configuration config;

    public void initialize(Configuration config) {
        //super.initialize(config);
        this.config = config;
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    protected abstract Logger getLogger();

    public void sinkStreamWC(DataStream<Tuple2<String, Integer>> dt) {
    }

    public void sinkStreamTM(DataStream<Tuple4<Date, Integer, Integer, Integer>> dt) {
    }

    public void sinkStreamSD(DataStream<Tuple4<String, Double, Double, String>> dt) {
    }

    public void sinkStreamSGOutlier(DataStream<Tuple4<Long, Long, String, Double>> dt) {
    }

    public void sinkStreamSGHouse(DataStream<Tuple3<Long, String, Double>> dt) {
    }

    public void sinkStreamSGPlug(DataStream<Tuple5<Long, String, String, String, Double>> dt) {
    }

    public void sinkStreamSA(DataStream<Tuple5<String, String, Date, String, Double>> dt) {
    }

    public void sinkStreamMO(DataStream<Tuple5<String, Double, Long, Boolean, Object>> dt) {
    }

    public void sinkStreamFD(DataStream<Tuple3<String, Double, String>> dt) {
    }

    public void createSinkLPVol(DataStream<Tuple2<Long, Long>> dt) {
    }

    public void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> dt) {
    }

    public void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt) {
    }

    public void createSinkCAStatus(DataStream<Tuple2<Integer, Integer>> dt) {
    }

    public void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt) {
    }

    public void createSinkAA(DataStream<Tuple6<String, String, Double, Long, Long, Integer>> dt) {
    }

    public void createSinkBI(DataStream<Tuple4<String, Double, Integer, Double>> dt) {
    }

    public void createSinkRL(DataStream<Tuple2<String, String[]>> dt) {
    }

    public void createSinkSF(DataStream<Tuple3<String,Float,Boolean>> dt) {
    }

    public void createSinkTT(DataStream<Tuple1<Rankings>> dt) {
    }

    public void createSinkVS(DataStream<Tuple4<String, Long, Double, CallDetailRecord>> dt) {
    }

    public void sinkStreamYSB(SingleOutputStreamOperator<Aggregate_Event> dt) {
    }
}
