package org.dspbench.applications.smartgrid;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

public class PlugLoadPredictorOperator extends LoadPredictorOperator {
    @Override
    protected String getKey(Tuple tuple) {
        return String.format("%s:%s:%s", tuple.getString(SmartGridConstants.Field.HOUSE_ID),
                tuple.getString(SmartGridConstants.Field.HOUSEHOLD_ID), tuple.getString(SmartGridConstants.Field.PLUG_ID));
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        String[] segments = keyString.split(":");
        return new Values(predictedTimeStamp, segments[0], segments[1], segments[2], predictedValue);
    }
}
