package org.dspbench.applications.smartgrid;

import org.dspbench.base.sink.BaseSink;
import org.dspbench.base.source.BaseSource;
import org.dspbench.base.task.AbstractTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.partitioning.Fields;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SmartGridTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(SmartGridTask.class);

    private BaseSource source;
    private BaseSink outlierSink;
    private BaseSink predictionSink;

    private int sourceThreads;
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

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);

        source = loadSource();
        outlierSink = loadSink("outlier");
        predictionSink = loadSink("prediction");

        sourceThreads = config.getInt(getConfigKey(SmartGridConstants.Config.SOURCE_THREADS), 1);
        outlierSinkThreads = config.getInt(getConfigKey(SmartGridConstants.Config.SINK_THREADS, "outlier"), 1);
        predictionSinkThreads = config.getInt(getConfigKey(SmartGridConstants.Config.SINK_THREADS, "prediction"), 1);

        slidingWindowThreads = config.getInt(SmartGridConstants.Config.SLIDING_WINDOW_THREADS, 1);
        globalMedianThreads = config.getInt(SmartGridConstants.Config.GLOBAL_MEDIAN_THREADS, 1);
        plugMedianThreads = config.getInt(SmartGridConstants.Config.PLUG_MEDIAN_THREADS, 1);
        outlierDetectorThreads = config.getInt(SmartGridConstants.Config.OUTLIER_DETECTOR_THREADS, 1);
        houseLoadThreads = config.getInt(SmartGridConstants.Config.HOUSE_LOAD_THREADS, 1);
        plugLoadThreads = config.getInt(SmartGridConstants.Config.PLUG_LOAD_THREADS, 1);

        houseLoadFrequency     = config.getInt(SmartGridConstants.Config.HOUSE_LOAD_FREQUENCY, 15);
        plugLoadFrequency      = config.getInt(SmartGridConstants.Config.PLUG_LOAD_FREQUENCY, 15);
    }

    public void initialize() {
        Stream smartmeters = builder.createStream(SmartGridConstants.Streams.SMARTMETERS, new Schema()
                .keys(SmartGridConstants.Field.ID)
                .fields(SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.VALUE, SmartGridConstants.Field.PROPERTY, SmartGridConstants.Field.PLUG_ID, SmartGridConstants.Field.HOUSEHOLD_ID, SmartGridConstants.Field.HOUSE_ID));

        Stream slidingWindow = builder.createStream(SmartGridConstants.Streams.SLIDING_WINDOW,
                new Schema(SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.HOUSEHOLD_ID, SmartGridConstants.Field.PLUG_ID, SmartGridConstants.Field.VALUE,
                        SmartGridConstants.Field.SLIDING_WINDOW_ACTION));

        Stream globalMedian = builder.createStream(SmartGridConstants.Streams.GLOBAL_MEDIAN, new Schema(SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.GLOBAL_MEDIAN_LOAD));
        Stream plugMedian = builder.createStream(SmartGridConstants.Streams.PLUG_MEDIAN, new Schema(SmartGridConstants.Field.PLUG_SPECIFIC_KEY, SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.PER_PLUG_MEDIAN));
        Stream outlierDetector = builder.createStream(SmartGridConstants.Streams.OUTLIER_DETECTOR, new Schema(SmartGridConstants.Field.SLIDING_WINDOW_START, SmartGridConstants.Field.SLIDING_WINDOW_END,
                SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.OUTLIER_PERCENTAGE));

        Stream houseLoad = builder.createStream(SmartGridConstants.Streams.HOUSE_LOAD, new Schema(SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.PREDICTED_LOAD));
        Stream plugLoad = builder.createStream(SmartGridConstants.Streams.PLUG_LOAD, new Schema(SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.HOUSEHOLD_ID,
                SmartGridConstants.Field.PLUG_ID, SmartGridConstants.Field.PREDICTED_LOAD));

        builder.setSource(SmartGridConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(SmartGridConstants.Component.SOURCE, smartmeters);

        builder.setOperator(SmartGridConstants.Component.SLIDING_WINDOW, new SmartGridSlidingWindowOperator(), slidingWindowThreads);
        builder.bcast(SmartGridConstants.Component.SLIDING_WINDOW, smartmeters);
        builder.publish(SmartGridConstants.Component.SLIDING_WINDOW, slidingWindow);

        // Outlier detection
        builder.setOperator(SmartGridConstants.Component.GLOBAL_MEDIAN, new GlobalMedianCalculatorOperator(), globalMedianThreads);
        builder.bcast(SmartGridConstants.Component.GLOBAL_MEDIAN, slidingWindow);
        builder.publish(SmartGridConstants.Component.GLOBAL_MEDIAN, globalMedian);

        builder.setOperator(SmartGridConstants.Component.PLUG_MEDIAN, new PlugMedianCalculatorOperator(), plugMedianThreads);
        builder.groupBy(SmartGridConstants.Component.PLUG_MEDIAN, slidingWindow, new Fields(SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.HOUSEHOLD_ID, SmartGridConstants.Field.PLUG_ID));
        builder.publish(SmartGridConstants.Component.PLUG_MEDIAN, plugMedian);

        builder.setOperator(SmartGridConstants.Component.OUTLIER_DETECTOR, new OutlierDetectionOperator(), outlierDetectorThreads);
        builder.bcast(SmartGridConstants.Component.OUTLIER_DETECTOR, globalMedian);
        builder.groupBy(SmartGridConstants.Component.OUTLIER_DETECTOR, plugMedian, new Fields(SmartGridConstants.Field.PLUG_SPECIFIC_KEY));
        builder.publish(SmartGridConstants.Component.OUTLIER_DETECTOR, outlierDetector);

        // Load prediction
        builder.setOperator(SmartGridConstants.Component.HOUSE_LOAD, new HouseLoadPredictorOperator(), houseLoadThreads);
        builder.groupBy(SmartGridConstants.Component.HOUSE_LOAD, smartmeters, new Fields(SmartGridConstants.Field.HOUSE_ID));
        builder.setTimer(SmartGridConstants.Component.HOUSE_LOAD, houseLoadFrequency, TimeUnit.SECONDS);
        builder.publish(SmartGridConstants.Component.HOUSE_LOAD, houseLoad);

        builder.setOperator(SmartGridConstants.Component.PLUG_LOAD, new PlugLoadPredictorOperator(), plugLoadThreads);
        builder.groupBy(SmartGridConstants.Component.PLUG_LOAD, smartmeters, new Fields(SmartGridConstants.Field.HOUSE_ID));
        builder.setTimer(SmartGridConstants.Component.PLUG_LOAD, plugLoadFrequency, TimeUnit.SECONDS);
        builder.publish(SmartGridConstants.Component.PLUG_LOAD, plugLoad);

        // Sinks
        builder.setOperator(SmartGridConstants.Component.OUTLIER_SINK, outlierSink, outlierSinkThreads);
        builder.shuffle(SmartGridConstants.Component.OUTLIER_SINK, outlierDetector);

        builder.setOperator(SmartGridConstants.Component.PREDICTION_SINK, predictionSink, predictionSinkThreads);
        builder.groupBy(SmartGridConstants.Component.PREDICTION_SINK, houseLoad, new Fields(SmartGridConstants.Field.HOUSE_ID));
        builder.groupBy(SmartGridConstants.Component.PREDICTION_SINK, plugLoad, new Fields(SmartGridConstants.Field.HOUSE_ID));
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
