package com.streamer.examples.machineoutlier.scorer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataInstanceScorerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DataInstanceScorerFactory.class);
    
    public static DataInstanceScorer getDataInstanceScorer(String dataTypeName) {
        if (dataTypeName.equals("machineMetadata")) {
            return new MachineDataInstanceScorer();
        } else {
            LOG.error("No matched data type scorer for {}", dataTypeName);
            throw new IllegalArgumentException("No matched data type scorer for " + dataTypeName);
        }
    }
}
