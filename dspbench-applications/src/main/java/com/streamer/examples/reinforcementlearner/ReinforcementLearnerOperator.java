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

package com.streamer.examples.reinforcementlearner;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.reinforcementlearner.ReinforcementLearnerConstants.*;
import com.streamer.examples.reinforcementlearner.learner.ReinforcementLearner;
import com.streamer.examples.reinforcementlearner.learner.ReinforcementLearnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reinforcement learner bolt. Any RL algorithm can be used
 * @author pranab
 *
 */
public class ReinforcementLearnerOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(ReinforcementLearnerOperator.class);

    private ReinforcementLearner learner;

    @Override
    public void initialize() {
        String learnerType = config.getString(Config.LEARNER_TYPE);
        String[] actions   = config.getString(Config.LEARNER_ACTIONS).split(",");
        
        learner = ReinforcementLearnerFactory.create(learnerType, actions, config);
    }

    public void process(Tuple input) {
        if (input.getStreamId().equals(Streams.EVENTS)) {
            // select action for next round
            String eventID = input.getString(Field.EVENT_ID);
            long roundNum   = input.getLong(Field.ROUND_NUM);
            
            String[] actions = learner.nextActions(roundNum);
            emit(input, new Values(eventID, actions));
        }
        
        else if (input.getStreamId().equals(Streams.REWARDS)) {
            // reward feedback
            String action = input.getString(Field.ACTION_ID);
            int reward    = input.getInt(Field.REWARD);
            
            learner.setReward(action, reward);
        }
    }
}
