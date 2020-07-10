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


package com.streamer.examples.reinforcementlearner.learner;

import com.streamer.utils.Configuration;

/**
 * Factory to create reinforcement learner
 * @author pranab
 *
 */
public class ReinforcementLearnerFactory {
    /**
     * @param learnerID
     * @param actions
     * @param config
     * @return
     */
    public static ReinforcementLearner create(String learnerID, String[] actions, Configuration config) {
        ReinforcementLearner learner = null;
        
        if (learnerID.equals("intervalEstimator")) {
            learner = new IntervalEstimator();
        } else if (learnerID.equals("sampsonSampler")) {
            learner = new SampsonSampler();
        } else if (learnerID.equals("optimisticSampsonSampler")) {
            learner = new OptimisticSampsonSampler();
        } else if (learnerID.equals("randomGreedyLearner")) {
            learner = new RandomGreedyLearner();
        }
        
        if (learner != null)
            learner.withActions(actions).initialize(config);
        
        return learner;
    }
}
