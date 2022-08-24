package org.dspbench.applications.smartgrid;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Author: Thilina
 * Date: 10/31/14
 */
public class HouseLoadPredictorBolt extends LoadPredictorBolt {

    public HouseLoadPredictorBolt() {
        super();
    }

    public HouseLoadPredictorBolt(int emitFrequencyInSeconds) {
        super(emitFrequencyInSeconds);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(SmartGridConstants.Field.TIMESTAMP, SmartGridConstants.Field.HOUSE_ID, SmartGridConstants.Field.PREDICTED_LOAD, SmartGridConstants.Field.INITTIME);
    }

    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getStringByField(SmartGridConstants.Field.HOUSE_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        return new Values(predictedTimeStamp, keyString, predictedValue);
    }
}