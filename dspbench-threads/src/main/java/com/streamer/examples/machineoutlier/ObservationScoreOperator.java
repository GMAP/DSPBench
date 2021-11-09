package com.streamer.examples.machineoutlier;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Config;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Field;
import com.streamer.examples.machineoutlier.scorer.DataInstanceScorer;
import com.streamer.examples.machineoutlier.scorer.DataInstanceScorerFactory;
import com.streamer.examples.machineoutlier.scorer.ScorePackage;
import java.util.ArrayList;
import java.util.List;

public class ObservationScoreOperator extends BaseOperator {
    private long previousTimestamp;
    private String dataTypeName;
    private DataInstanceScorer dataInstanceScorer;
    private List<Object> observationList;
    private Tuple parentTuple;

    @Override
    public void initialize() {
        previousTimestamp  = 0;
        dataTypeName       = config.getString(Config.SCORER_DATA_TYPE);
        observationList    = new ArrayList<Object>();
        dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public void process(Tuple input) {
        long timestamp = input.getLong(Field.TIMESTAMP);
        
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit 
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                
                for (ScorePackage scorePackage : scorePackageList) {
                    emit(parentTuple, new Values(scorePackage.getId(), scorePackage.getScore(), 
                            previousTimestamp, scorePackage.getObj()));
                }
                
                observationList.clear();
            }

            previousTimestamp = timestamp;
        }

        if (observationList.isEmpty()) {
            parentTuple = input;
        }
        observationList.add(input.getValue(Field.OBSERVATION));
    }
}
