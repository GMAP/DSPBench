package flink.application;

import flink.application.YSB.Aggregate_Event;
import flink.application.voipstream.CallDetailRecord;
import flink.constants.BaseConstants;
import flink.sink.BaseSink;
import flink.source.BaseSource;
import flink.tools.Rankings;
import flink.util.ClassLoaderUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.BooleanValue;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.Date;

/**
 *
 * @author gabrielfim
 */
public abstract class AbstractApplication implements Serializable {
    protected String appName;
    protected Configuration config;
    protected transient StreamExecutionEnvironment env;

    public AbstractApplication(String appName, Configuration config) {
        this.appName = appName;
        this.config = config;
        this.env = new StreamExecutionEnvironment();
    }

    public abstract void initialize();

    public abstract StreamExecutionEnvironment buildApplication();

    public abstract String getConfigPrefix();

    public abstract Logger getLogger();

    public String getAppName() {
        return appName;
    }

    protected DataStream<String> createSource() {
        String sourceClass = config.getString(getConfigKey(BaseConstants.BaseConf.SOURCE_CLASS),
                "flink.source.FileSource");
        BaseSource source = (BaseSource) ClassLoaderUtils.newInstance(sourceClass, "source", getLogger());
        source.initialize(config, env, getConfigPrefix());
        return source.createStream();
    }

    protected DataStream<String> createSource(String name) {
        String sourceClass = config.getString(getConfigKey(BaseConstants.BaseConf.SOURCE_CLASS, name),
                "flink.source.FileSource");
        BaseSource source = (BaseSource) ClassLoaderUtils.newInstance(sourceClass, "source", getLogger());
        source.initialize(config, env, String.format("%s.%s", getConfigPrefix(), name));
        return source.createStream();
    }

    protected void createSinkWC(DataStream<Tuple2<String, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamWC(dt);
    }

    protected void createSinkTM(DataStream<Tuple4<Date, Integer, Integer, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamTM(dt);
    }

    protected void createSinkSD(DataStream<Tuple4<String, Double, Double, String>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamSD(dt);
    }

    protected void createSinkSA(DataStream<Tuple5<String, String, Date, String, Double>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamSA(dt);
    }

    protected void createSinkFD(DataStream<Tuple3<String, Double, String>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamFD(dt);
    }

    protected void createSinkMO(DataStream<Tuple5<String, Double, Long, Boolean, Object>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamMO(dt);
    }

    protected void createSinkSGOutlier(DataStream<Tuple4<Long, Long, String, Double>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamSGOutlier(dt);
    }

    protected void createSinkSGHouse(DataStream<Tuple3<Long, String, Double>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamSGHouse(dt);
    }

    protected void createSinkSGPlug(DataStream<Tuple5<Long, String, String, String, Double>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamSGPlug(dt);
    }

    protected void createSinkLPVol(DataStream<Tuple2<Long, Long>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkLPVol(dt);
    }

    protected void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkLPStatus(dt);
    }

    protected void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkLPGeo(dt);
    }

    protected void createSinkCAStatus(DataStream<Tuple2<Integer, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkCAStatus(dt);
    }

    protected void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkCAGeo(dt);
    }

    protected void createSinkAA(DataStream<Tuple6<String, String, Double, Long, Long, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkAA(dt);
    }

    protected void createSinkBI(DataStream<Tuple4<String, Double, Integer, Double>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkBI(dt);
    }

    protected void createSinkRL(DataStream<Tuple2<String, String[]>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkRL(dt);
    }

    protected void createSinkSF(DataStream<Tuple3<String,Float,Boolean>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkSF(dt);
    }

    protected void createSinkTT(DataStream<Tuple1<Rankings>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkTT(dt);
    }

    protected void createSinkVS(DataStream<Tuple4<String, Long, Double, CallDetailRecord>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.createSinkVS(dt);
    }

    protected void createSinkYSB(SingleOutputStreamOperator<Aggregate_Event> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.initialize(config);
        sink.sinkStreamYSB(dt);
    }

    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     * 
     * @param key  The configuration key to be parsed
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected String getConfigKey(String key, String name) {
        return String.format(key, String.format("%s.%s", getConfigPrefix(), name));
    }

    /**
     * Utility method to parse a configuration key with the application prefix..
     * 
     * @param key The configuration key to be parsed
     * @return
     */
    protected String getConfigKey(String key) {
        return String.format(key, getConfigPrefix());
    }
}
