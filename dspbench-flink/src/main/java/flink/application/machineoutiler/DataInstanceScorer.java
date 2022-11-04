package flink.application.machineoutiler;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.operators.shipping.OutputCollector;

import java.util.List;

/**
 * DataInstanceScorer defines the method to calculate the data instance anomaly scores.
 * @author yexijiang
 * @param <T>
 *
 */
public abstract class DataInstanceScorer<T> {
    /**
     * Emit the calculated score to downstream.
     * @param collector
     * @param observationList
     */
    public void calculateScores(OutputCollector collector, List<T> observationList) {
        List<ScorePackage> packageList = getScores(observationList);
        for (ScorePackage scorePackage : packageList) {
            //There is no .emit
            collector.collect(new Tuple3<String,Double,Object>(scorePackage.getId(), scorePackage.getScore(), scorePackage.getObj()));
        }
    }

    /**
     * Calculate the data instance anomaly score for given data instances and directly send to downstream.
     * @param observationList
     * @return
     */
    public abstract List<ScorePackage> getScores(List<T> observationList);
}
