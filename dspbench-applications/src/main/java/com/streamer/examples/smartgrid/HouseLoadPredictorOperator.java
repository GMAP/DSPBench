package com.streamer.examples.smartgrid;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.smartgrid.SmartGridConstants.*;

public class HouseLoadPredictorOperator extends LoadPredictorOperator {
    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getString(Field.HOUSE_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        return new Values(predictedTimeStamp, keyString, predictedValue);
    }
}
