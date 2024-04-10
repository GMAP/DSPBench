package flink.application;

import flink.application.YSB.Aggregate_Event;
import flink.constants.BaseConstants;
import flink.sink.BaseSink;
import flink.source.BaseSource;
import flink.util.ClassLoaderUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    protected void createSinkWC(DataStream<Tuple2<String, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamWC(dt);
    }

    protected void createSinkTM(DataStream<Tuple4<Date, Integer, Integer, Integer>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamTM(dt);
    }

    protected void createSinkSD(DataStream<Tuple4<String, Double, Double, String>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamSD(dt);
    }

    protected void createSinkSA(DataStream<Tuple5<String, String, Date, String, Double>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamSA(dt);
    }

    protected void createSinkFD(DataStream<Tuple4<String, Double, String, String>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamFD(dt);
    }

    protected void createSinkMO(DataStream<Tuple6<String, Double, Long, Boolean, Object, String>> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamMO(dt);
    }

    protected void createSinkSGOutlier(DataStream<Tuple5<Long, Long, String, Double, String>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamSGOutlier(dt, sinkName);
    }

    protected void createSinkSGHouse(DataStream<Tuple4<Long, String, Double, String>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamSGHouse(dt, sinkName);
    }

    protected void createSinkSGPlug(DataStream<Tuple6<Long, String, String, String, Double, String>> dt,
            String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamSGPlug(dt, sinkName);
    }

    protected void createSinkLPVol(DataStream<Tuple2<Long, Long>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.createSinkLPVol(dt, sinkName);
    }

    protected void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.createSinkLPStatus(dt, sinkName);
    }

    protected void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.createSinkLPGeo(dt, sinkName);
    }

    protected void createSinkCAStatus(DataStream<Tuple2<Integer, Integer>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.createSinkCAStatus(dt, sinkName);
    }

    protected void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt, String sinkName) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.createSinkCAGeo(dt, sinkName);
    }

    protected void createSinkYSB(SingleOutputStreamOperator<Aggregate_Event> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS), "flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStreamYSB(dt);
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
