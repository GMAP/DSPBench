/*
 * avenir: Predictive analytic based on Hadoop Map Reduce
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package flink.application.reinforcementlearner.learner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.ReinforcementLearnerConstants;

import org.apache.flink.configuration.Configuration;
import flink.util.HistogramStat;


/**
 * Interval estimator reinforcement learner based on confidence bound
 * @author pranab
 *
 */
public class IntervalEstimator extends ReinforcementLearnerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(IntervalEstimator.class);
    
    private int binWidth;
    private int confidenceLimit;
    private int minConfidenceLimit;
    private int curConfidenceLimit;
    private int confidenceLimitReductionStep;
    private int confidenceLimitReductionRoundInterval;
    private int minDistrSample;
    private int lastRoundNum = 1;
    private long randomSelectCount;
    private long intvEstSelectCount;
    private long logCounter;
    private long roundCounter;
    private boolean lowSample = true;
    private boolean debugOn;
    
    private Map<String, HistogramStat> rewardDistr = new HashMap<>(); 
    

    @Override
    public void initialize(Configuration config) {
        binWidth = config.getInteger(ReinforcementLearnerConstants.Conf.BIN_WIDTH, 1);
        confidenceLimit = config.getInteger(ReinforcementLearnerConstants.Conf.CONFIDENCE_LIMIT, 95);
        minConfidenceLimit = config.getInteger(ReinforcementLearnerConstants.Conf.MIN_CONFIDENCE_LIMIT, 50);
        curConfidenceLimit = confidenceLimit;
        confidenceLimitReductionStep = config.getInteger(ReinforcementLearnerConstants.Conf.CONFIDENCE_LIMIT_RED_STEP, 5);
        confidenceLimitReductionRoundInterval = config.getInteger(ReinforcementLearnerConstants.Conf.CONFIDENCE_LIMIT_RED_ROUND_INT, 50);
        minDistrSample = config.getInteger(ReinforcementLearnerConstants.Conf.MIN_DIST_SAMPLE, 30);
        debugOn = config.getBoolean(ReinforcementLearnerConstants.Conf.DEBUG_ON, false);

        for (String action : actions) {
            rewardDistr.put(action, new HistogramStat(binWidth));
        }

        initSelectedActions();

        if (debugOn) {
            LOG.info("confidenceLimit:" + confidenceLimit + " minConfidenceLimit:" 
                    + minConfidenceLimit + " confidenceLimitReductionStep:" 
                    + confidenceLimitReductionStep + " confidenceLimitReductionRoundInterval:" 
                    + confidenceLimitReductionRoundInterval + " minDistrSample:" + minDistrSample);
        }
    }

    @Override
    public String[] nextActions(int roundNum) {
        String selAction = null;
        ++logCounter;
        ++roundCounter;
        //make sure reward distributions have enough sample
        if (lowSample) {
            lowSample = false;
            for (String action : rewardDistr.keySet()) {
                int sampleCount = rewardDistr.get(action).getCount();
                if (debugOn && logCounter % 100 == 0) {
                    LOG.info("action:" + action + " distr sampleCount: " + sampleCount);
                }
                if (sampleCount < minDistrSample) {
                    lowSample = true;
                    break;
                }
            }

            if (!lowSample && debugOn) {
                LOG.info("got full sample");
                lastRoundNum = roundNum;
            }
        }

        if (lowSample) {
            //select randomly
            selAction = actions[(int)(Math.random() * actions.length)];
            ++randomSelectCount;
        } else {
            //reduce confidence limit
            adjustConfLimit(roundNum);

            //select as per interval estimate, choosing distr with max upper conf bound
            int maxUpperConfBound = 0;
            for (String action : rewardDistr.keySet()) {
                HistogramStat stat = rewardDistr.get(action);
                int[] confBounds = stat.getConfidenceBounds(curConfidenceLimit);
                if (debugOn) {
                    LOG.info("curConfidenceLimit:" + curConfidenceLimit + " action:" + action + " conf bounds:" + confBounds[0] + "  " + confBounds[1]);
                }
                if (confBounds[1] > maxUpperConfBound) {
                    maxUpperConfBound = confBounds[1];
                    selAction = action;
                }
            }
            ++intvEstSelectCount;
        }
        selActions[0] = selAction;
        return selActions;
    }

    /**
     * @param roundNum
     */
    private void adjustConfLimit(int roundNum) {
        if (curConfidenceLimit > minConfidenceLimit) {
            int redStep = (roundNum - lastRoundNum) / confidenceLimitReductionRoundInterval;
            if (debugOn) {
                LOG.info("redStep:" +  redStep + " roundNum:"  + roundNum + " lastRoundNum:" + lastRoundNum);
            }
            
            if (redStep > 0) {
                curConfidenceLimit -=  (redStep * confidenceLimitReductionStep);
                if (curConfidenceLimit < minConfidenceLimit) {
                    curConfidenceLimit = minConfidenceLimit;
                }
                if (debugOn) {
                    LOG.info("reduce conf limit roundNum:" +  roundNum + " lastRoundNum:"  + lastRoundNum);
                }
                lastRoundNum = roundNum;
            }
        }
    }

    @Override
    public void setReward(String action, int reward) {
        HistogramStat stat = rewardDistr.get(action);
        if (null == stat) {
            throw new IllegalArgumentException("invalid action:" + action);
        }
        stat.add(reward);

        if (debugOn) {
            LOG.info("setReward action:" + action + " reward:" + reward + " sample count:" + stat.getCount());
        }
    }

    @Override
    public String getStat() {
        return "randomSelectCount:" + randomSelectCount + " intvEstSelectCount:" + intvEstSelectCount; 
    }
}
