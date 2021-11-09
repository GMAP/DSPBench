package org.dspbench.applications.smartgrid;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.constants.SmartGridConstants;
import org.dspbench.sink.BaseSink;
import org.dspbench.source.AbstractSpout;
import org.dspbench.topology.AbstractTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class SmartGridTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SmartGridTopology.class);
    
    private AbstractSpout spout;
    private BaseSink outlierSink;
    private BaseSink predictionSink;
    
    private int spoutThreads;
    private int outlierSinkThreads;
    private int predictionSinkThreads;
    private int slidingWindowThreads;
    private int globalMedianThreads;
    private int plugMedianThreads;
    private int outlierDetectorThreads;
    private int houseLoadThreads;
    private int plugLoadThreads;
    
    private int houseLoadFrequency;
    private int plugLoadFrequency;

    public SmartGridTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        spout          = loadSpout();
        outlierSink    = loadSink("outlier");
        predictionSink = loadSink("prediction");
        
        spoutThreads           = config.getInt(getConfigKey(SmartGridConstants.Conf.SPOUT_THREADS), 1);
        outlierSinkThreads     = config.getInt(getConfigKey(SmartGridConstants.Conf.SINK_THREADS, "outlier"), 1);
        predictionSinkThreads  = config.getInt(getConfigKey(SmartGridConstants.Conf.SINK_THREADS, "prediction"), 1);
        
        slidingWindowThreads   = config.getInt(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        globalMedianThreads    = config.getInt(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        plugMedianThreads      = config.getInt(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        outlierDetectorThreads = config.getInt(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        houseLoadThreads       = config.getInt(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        plugLoadThreads        = config.getInt(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        
        houseLoadFrequency     = config.getInt(SmartGridConstants.Conf.HOUSE_LOAD_FREQUENCY, 15);
        plugLoadFrequency      = config.getInt(SmartGridConstants.Conf.PLUG_LOAD_FREQUENCY, 15);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(SmartGridConstants.Field.ID, SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.VALUE, SmartGridConstants.Field.PROPERTY,
                                   SmartGridConstants.Field.PLUG_ID, SmartGridConstants.Field.HOUSEHOLD_ID, SmartGridConstants.Field.HOUSE_ID));
        
        builder.setSpout(SmartGridConstants.Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(SmartGridConstants.Component.SLIDING_WINDOW, new SmartGridSlidingWindowBolt(), slidingWindowThreads)
               .globalGrouping(SmartGridConstants.Component.SPOUT);
        
        // Outlier detection
        builder.setBolt(SmartGridConstants.Component.GLOBAL_MEDIAN, new GlobalMedianCalculatorBolt(), globalMedianThreads)
               .globalGrouping(SmartGridConstants.Component.SLIDING_WINDOW);
        
        builder.setBolt(SmartGridConstants.Component.PLUG_MEDIAN, new PlugMedianCalculatorBolt(), plugMedianThreads)
               .fieldsGrouping(SmartGridConstants.Component.SLIDING_WINDOW, new Fields(SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.HOUSEHOLD_ID, SmartGridConstants.Field.PLUG_ID));
        
        builder.setBolt(SmartGridConstants.Component.OUTLIER_DETECTOR, new OutlierDetectionBolt(), outlierDetectorThreads)
               .allGrouping(SmartGridConstants.Component.GLOBAL_MEDIAN)
               .fieldsGrouping(SmartGridConstants.Component.PLUG_MEDIAN, new Fields(SmartGridConstants.Field.PLUG_SPECIFIC_KEY));
        
        // Load prediction
        builder.setBolt(SmartGridConstants.Component.HOUSE_LOAD, new HouseLoadPredictorBolt(houseLoadFrequency), houseLoadThreads)
               .fieldsGrouping(SmartGridConstants.Component.SPOUT, new Fields(SmartGridConstants.Field.HOUSE_ID));
        
        builder.setBolt(SmartGridConstants.Component.PLUG_LOAD, new PlugLoadPredictorBolt(plugLoadFrequency), plugLoadThreads)
               .fieldsGrouping(SmartGridConstants.Component.SPOUT, new Fields(SmartGridConstants.Field.HOUSE_ID));
        
        // Sinks
        builder.setBolt(SmartGridConstants.Component.OUTLIER_SINK, outlierSink, outlierSinkThreads)
               .shuffleGrouping(SmartGridConstants.Component.OUTLIER_DETECTOR);
        
        builder.setBolt(SmartGridConstants.Component.PREDICTION_SINK, predictionSink, predictionSinkThreads)
               .fieldsGrouping(SmartGridConstants.Component.HOUSE_LOAD, new Fields(SmartGridConstants.Field.HOUSE_ID))
               .fieldsGrouping(SmartGridConstants.Component.PLUG_LOAD, new Fields(SmartGridConstants.Field.HOUSE_ID));
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return SmartGridConstants.PREFIX;
    }
    
}
