package org.dspbench.applications.machineoutlier;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.applications.machineoutlier.scorer.DataInstanceScorer;
import org.dspbench.applications.machineoutlier.scorer.DataInstanceScorerFactory;
import org.dspbench.applications.machineoutlier.scorer.ScorePackage;

public class ObservationScoreBolt extends AbstractBolt {
    private long previousTimestamp;
    private String dataTypeName;
    private DataInstanceScorer dataInstanceScorer;
    private List<Object> observationList;

    @Override
    public void initialize() {
        previousTimestamp = 0;
        dataTypeName = config.getString(MachineOutlierConstants.Conf.SCORER_DATA_TYPE);
        observationList = new ArrayList<>();
        dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(MachineOutlierConstants.Field.TIMESTAMP);
        
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit 
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    collector.emit(new Values(scorePackage.getId(), scorePackage.getScore(), 
                            previousTimestamp, scorePackage.getObj(), input.getStringByField(MachineOutlierConstants.Field.INITTIME)));
                }
                observationList.clear();
            }

            previousTimestamp = timestamp;
        }

        observationList.add(input.getValueByField(MachineOutlierConstants.Field.OBSERVATION));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(MachineOutlierConstants.Field.ID, MachineOutlierConstants.Field.DATAINST_ANOMALY_SCORE, MachineOutlierConstants.Field.TIMESTAMP, MachineOutlierConstants.Field.OBSERVATION,  MachineOutlierConstants.Field.INITTIME);
    }
}
