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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;

import flink.constants.ReinforcementLearnerConstants;
import flink.util.SimpleStat;

/**
 * Random greedy reinforcement learner
 * @author pranab
 *
 */
public class RandomGreedyLearner extends ReinforcementLearnerInterface {
    private static final String PROB_RED_LINEAR = "linear";
    private static final String PROB_RED_LOG_LINEAR = "logLinear";
    
    private double randomSelectionProb;
    private String probRedAlgorithm;
    private double probReductionConstant;
    
    private Map<String, SimpleStat> rewardStats = new HashMap<>();
    
    @Override
    public void initialize(Configuration config) {
        randomSelectionProb   = config.getDouble(ReinforcementLearnerConstants.Conf.RANDOM_SELECTION_PROB, 0.5);
        probRedAlgorithm      = config.getString(ReinforcementLearnerConstants.Conf.PROB_RED_ALGORITHM, PROB_RED_LINEAR );
        probReductionConstant = config.getDouble(ReinforcementLearnerConstants.Conf.PROB_RED_CONSTANT,  1.0);

        for (String action : actions) {
            rewardStats.put(action, new SimpleStat());
        }
    }

    @Override
    public String[] nextActions(int roundNum) {
        double curProb = 0.0;
        String action = null;
        
        if (probRedAlgorithm.equals(PROB_RED_LINEAR )) {
            curProb = randomSelectionProb * probReductionConstant / roundNum ;
        } else {
            curProb = randomSelectionProb * probReductionConstant * Math.log(roundNum) / roundNum;
        }
        curProb = curProb <= randomSelectionProb ? curProb : randomSelectionProb;

        if (curProb < Math.random()) {
            //select random
            action = actions[(int)(Math.random() * actions.length)];
        } else {
            //select best
            int bestReward = 0;
            
            for (String thisAction : actions) {
                int thisReward = (int)(rewardStats.get(thisAction).getMean());
                if (thisReward >  bestReward) {
                    bestReward = thisReward;
                    action = thisAction;
                }
            }
        }

        selActions[0] = action;
        return selActions;
    }

    @Override
    public void setReward(String action, int reward) {
        rewardStats.get(action).add(reward);
    }
}
