package org.dspbench.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import static org.dspbench.constants.MachineOutlierConstants.*;
import org.dspbench.model.metadata.MachineMetadata;
import org.dspbench.util.math.Entropy;
import org.dspbench.util.math.MaximumLikelihoodNormalDistribution;

public class DataInstancesScoreBolt extends AbstractBolt {
    private long previousTimestamp;
    private Map<Double, List<String>> histogram; // histogram for a batch of data
                                                                                                                                                                                            // instances.
    private int totalCountInBatch; // total count for a batch of data instances.

    @Override
    public void initialize() {
        previousTimestamp = 0;
        histogram = new HashMap<>();
        totalCountInBatch = 0;
    }

    @Override
    public void execute(Tuple input) {
        long curTimestamp = input.getLongByField(Field.TIMESTAMP);
        String machineIp = input.getStringByField(Field.ID);
        
        if (curTimestamp != previousTimestamp && totalCountInBatch != 0) { 
            // score data instances of previous batch
            MaximumLikelihoodNormalDistribution mlnd = new MaximumLikelihoodNormalDistribution(
                    totalCountInBatch, histogram);

            double minIdle = Double.MAX_VALUE;
            for (Double v : histogram.keySet()) {
                if (v < minIdle) {
                    minIdle = v;
                }
            }

            double entropy = Entropy.calculateEntropyNormalDistribution(mlnd.getSigma());

            StringBuilder blankLines = new StringBuilder();
            for (int i = 0; i < 20; ++i) {
                blankLines.append("\n");
            }

            // emit to stream score bolt
            Set<Double> keySet = histogram.keySet();
            for (double key : keySet) {
                List<String> entityList = histogram.get(key);
                String firstEntity = entityList.remove(0);

                // estimate parameters for leave-one-out histogram
                MaximumLikelihoodNormalDistribution ml = new MaximumLikelihoodNormalDistribution(
                        totalCountInBatch - 1, histogram);
                
                double leaveOneOutEntropy = Entropy.calculateEntropyNormalDistribution(ml.getSigma());
                double entropyReduce = entropy - leaveOneOutEntropy;
                entropyReduce = entropyReduce > 0 ? entropyReduce : 0;
                double score = entropyReduce * totalCountInBatch;

                // put the removed one back to histogram
                entityList.add(firstEntity);

                for (String entityId : entityList) {
                    collector.emit(new Values(entityId, curTimestamp, score));
                }
            }

            histogram.clear();
            totalCountInBatch = 0;
            previousTimestamp = curTimestamp;
        }

        MachineMetadata machineMetaData = (MachineMetadata) input.getValue(2);
        double idleTime = machineMetaData.getCpuIdleTime();
        idleTime = idleTime > 0 ? idleTime / 100000 * 100000 : 0;
        List<String> instancesList = histogram.get(idleTime);
        if (instancesList == null) {
            instancesList = new ArrayList<>();
        }
        
        instancesList.add(machineIp);
        histogram.put(idleTime, instancesList);
        ++totalCountInBatch;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ENTITY_ID, Field.TIMESTAMP, Field.DATAINST_SCORE);
    }
}