package org.dspbench.applications.smartgrid;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

public class HouseLoadPredictorOperator extends LoadPredictorOperator {
    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getString(SmartGridConstants.Field.HOUSE_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        return new Values(predictedTimeStamp, keyString, predictedValue);
    }
}
