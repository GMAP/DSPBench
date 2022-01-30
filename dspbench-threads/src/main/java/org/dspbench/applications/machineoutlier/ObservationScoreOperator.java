package org.dspbench.applications.machineoutlier;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.machineoutlier.scorer.DataInstanceScorer;
import org.dspbench.applications.machineoutlier.scorer.DataInstanceScorerFactory;
import org.dspbench.applications.machineoutlier.scorer.ScorePackage;

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
        dataTypeName       = config.getString(MachineOutlierConstants.Config.SCORER_DATA_TYPE);
        observationList    = new ArrayList<Object>();
        dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public void process(Tuple input) {
        long timestamp = input.getLong(MachineOutlierConstants.Field.TIMESTAMP);
        
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
        observationList.add(input.getValue(MachineOutlierConstants.Field.OBSERVATION));
    }
}
