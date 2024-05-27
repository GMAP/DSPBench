package spark.streaming.model.scorer;

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

    /**
     * Calculate the data instance anomaly score for given data instances and directly send to downstream.
     * @param observationList
     * @return
     */
    public abstract List<ScorePackage> getScores(List<T> observationList);
}
