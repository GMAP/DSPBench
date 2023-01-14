package flink.application.machineoutiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataInstanceScorerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DataInstanceScorerFactory.class);
    
    public static DataInstanceScorer getDataInstanceScorer(String dataTypeName) {
        if (dataTypeName.equals("machineMetadata")) {
            return new MachineDataInstanceScorer();
        }
        LOG.error("{} is not a valid data type for the Scorer", dataTypeName);
        return null;
    }
}
