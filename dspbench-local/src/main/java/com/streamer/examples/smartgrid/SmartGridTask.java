package com.streamer.examples.smartgrid;

import com.streamer.base.sink.BaseSink;
import com.streamer.base.source.BaseSource;
import com.streamer.base.task.AbstractTask;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.examples.smartgrid.SmartGridConstants.*;
import com.streamer.partitioning.Fields;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import static com.streamer.examples.smartgrid.SmartGridConstants.PREFIX;

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

        sourceThreads = config.getInt(getConfigKey(Config.SOURCE_THREADS), 1);
        outlierSinkThreads = config.getInt(getConfigKey(Config.SINK_THREADS, "outlier"), 1);
        predictionSinkThreads = config.getInt(getConfigKey(Config.SINK_THREADS, "prediction"), 1);

        slidingWindowThreads = config.getInt(Config.SLIDING_WINDOW_THREADS, 1);
        globalMedianThreads = config.getInt(Config.GLOBAL_MEDIAN_THREADS, 1);
        plugMedianThreads = config.getInt(Config.PLUG_MEDIAN_THREADS, 1);
        outlierDetectorThreads = config.getInt(Config.OUTLIER_DETECTOR_THREADS, 1);
        houseLoadThreads = config.getInt(Config.HOUSE_LOAD_THREADS, 1);
        plugLoadThreads = config.getInt(Config.PLUG_LOAD_THREADS, 1);

        houseLoadFrequency     = config.getInt(Config.HOUSE_LOAD_FREQUENCY, 15);
        plugLoadFrequency      = config.getInt(Config.PLUG_LOAD_FREQUENCY, 15);
    }

    public void initialize() {
        Stream smartmeters = builder.createStream(Streams.SMARTMETERS, new Schema()
                .keys(Field.ID)
                .fields(Field.TIMESTAMP, Field.VALUE, Field.PROPERTY, Field.PLUG_ID, Field.HOUSEHOLD_ID, Field.HOUSE_ID));

        Stream slidingWindow = builder.createStream(Streams.SLIDING_WINDOW,
                new Schema(Field.TIMESTAMP, Field.HOUSE_ID, Field.HOUSEHOLD_ID, Field.PLUG_ID, Field.VALUE,
                        Field.SLIDING_WINDOW_ACTION));

        Stream globalMedian = builder.createStream(Streams.GLOBAL_MEDIAN, new Schema(Field.TIMESTAMP, Field.GLOBAL_MEDIAN_LOAD));
        Stream plugMedian = builder.createStream(Streams.PLUG_MEDIAN, new Schema(Field.PLUG_SPECIFIC_KEY, Field.TIMESTAMP, Field.PER_PLUG_MEDIAN));
        Stream outlierDetector = builder.createStream(Streams.OUTLIER_DETECTOR, new Schema(Field.SLIDING_WINDOW_START, Field.SLIDING_WINDOW_END,
                Field.HOUSE_ID, Field.OUTLIER_PERCENTAGE));

        Stream houseLoad = builder.createStream(Streams.HOUSE_LOAD, new Schema(Field.TIMESTAMP, Field.HOUSE_ID, Field.PREDICTED_LOAD));
        Stream plugLoad = builder.createStream(Streams.PLUG_LOAD, new Schema(Field.TIMESTAMP, Field.HOUSE_ID, Field.HOUSEHOLD_ID,
                Field.PLUG_ID, Field.PREDICTED_LOAD));

        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, smartmeters);

        builder.setOperator(Component.SLIDING_WINDOW, new SmartGridSlidingWindowOperator(), slidingWindowThreads);
        builder.bcast(Component.SLIDING_WINDOW, smartmeters);
        builder.publish(Component.SLIDING_WINDOW, slidingWindow);

        // Outlier detection
        builder.setOperator(Component.GLOBAL_MEDIAN, new GlobalMedianCalculatorOperator(), globalMedianThreads);
        builder.bcast(Component.GLOBAL_MEDIAN, slidingWindow);
        builder.publish(Component.GLOBAL_MEDIAN, globalMedian);

        builder.setOperator(Component.PLUG_MEDIAN, new PlugMedianCalculatorOperator(), plugMedianThreads);
        builder.groupBy(Component.PLUG_MEDIAN, slidingWindow, new Fields(Field.HOUSE_ID, Field.HOUSEHOLD_ID, Field.PLUG_ID));
        builder.publish(Component.PLUG_MEDIAN, plugMedian);

        builder.setOperator(Component.OUTLIER_DETECTOR, new OutlierDetectionOperator(), outlierDetectorThreads);
        builder.bcast(Component.OUTLIER_DETECTOR, globalMedian);
        builder.groupBy(Component.OUTLIER_DETECTOR, plugMedian, new Fields(Field.PLUG_SPECIFIC_KEY));
        builder.publish(Component.OUTLIER_DETECTOR, outlierDetector);

        // Load prediction
        builder.setOperator(Component.HOUSE_LOAD, new HouseLoadPredictorOperator(), houseLoadThreads);
        builder.groupBy(Component.HOUSE_LOAD, smartmeters, new Fields(Field.HOUSE_ID));
        builder.setTimer(Component.HOUSE_LOAD, houseLoadFrequency, TimeUnit.SECONDS);
        builder.publish(Component.HOUSE_LOAD, houseLoad);

        builder.setOperator(Component.PLUG_LOAD, new PlugLoadPredictorOperator(), plugLoadThreads);
        builder.groupBy(Component.PLUG_LOAD, smartmeters, new Fields(Field.HOUSE_ID));
        builder.setTimer(Component.PLUG_LOAD, plugLoadFrequency, TimeUnit.SECONDS);
        builder.publish(Component.PLUG_LOAD, plugLoad);

        // Sinks
        builder.setOperator(Component.OUTLIER_SINK, outlierSink, outlierSinkThreads);
        builder.shuffle(Component.OUTLIER_SINK, outlierDetector);

        builder.setOperator(Component.PREDICTION_SINK, predictionSink, predictionSinkThreads);
        builder.groupBy(Component.PREDICTION_SINK, houseLoad, new Fields(Field.HOUSE_ID));
        builder.groupBy(Component.PREDICTION_SINK, plugLoad, new Fields(Field.HOUSE_ID));
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
