package com.streamer.examples.machineoutlier.scorer;

import java.util.List;


/**
 * DataInstanceScorer defines the method to calculate the data instance anomaly scores.
 * @author yexijiang
 * @param <T>
 *
 */
public interface DataInstanceScorer<T> {
    /**
     * Calculate the data instance anomaly score for given data instances and directly send to downstream.
     * @param observationList
     * @return
     */
    public List<ScorePackage> getScores(List<T> observationList);
}
