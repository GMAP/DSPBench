package com.streamer.examples.smartgrid;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.smartgrid.SmartGridConstants.*;

public class PlugLoadPredictorOperator extends LoadPredictorOperator {
    @Override
    protected String getKey(Tuple tuple) {
        return String.format("%s:%s:%s", tuple.getString(Field.HOUSE_ID),
                tuple.getString(Field.HOUSEHOLD_ID), tuple.getString(Field.PLUG_ID));
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        String[] segments = keyString.split(":");
        return new Values(predictedTimeStamp, segments[0], segments[1], segments[2], predictedValue);
    }
}
