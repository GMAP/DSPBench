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

import com.streamer.base.task.AbstractTask;
import com.streamer.core.Operator;
import com.streamer.core.Schema;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import static com.streamer.examples.reinforcementlearner.ReinforcementLearnerConstants.*;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds and submits storm topology for reinforcement learning
 * @author pranab
 *
 */
public class ReinforcementLearnerTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(ReinforcementLearnerTask.class);

    private Source eventSource;
    private Source rewardSource;
    private Operator sink;
    
    private int eventSourceThreads;
    private int rewardSourceThreads;
    private int learnerThreads;
    private int sinkThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);

        eventSource  = loadSource("event");
        rewardSource = loadSource("reward");
        sink         = loadSink();
        
        eventSourceThreads  = config.getInt(getConfigKey(Config.SOURCE_THREADS, "event"), 1);
        rewardSourceThreads = config.getInt(getConfigKey(Config.SOURCE_THREADS, "reward"), 1);
        learnerThreads      = config.getInt(Config.LEARNER_THREADS, 1);
        sinkThreads         = config.getInt(Config.SINK_THREADS, 1);
    }

    public void initialize() {
        Stream events  = builder.createStream(Streams.EVENTS, new Schema(Field.EVENT_ID, Field.ROUND_NUM));
        Stream rewards = builder.createStream(Streams.REWARDS, new Schema(Field.ACTION_ID, Field.REWARD));
        Stream actions = builder.createStream(Streams.ACTIONS, new Schema(Field.EVENT_ID, Field.ACTIONS));
        
        builder.setSource(Component.EVENT_SOURCE, eventSource, eventSourceThreads);
        builder.publish(Component.EVENT_SOURCE, events);
        
        builder.setSource(Component.REWARD_SOURCE, rewardSource, rewardSourceThreads);
        builder.publish(Component.REWARD_SOURCE, rewards);
        
        builder.setOperator(Component.LEARNER, new ReinforcementLearnerOperator(), learnerThreads);
        builder.shuffle(Component.LEARNER, events);
        builder.bcast(Component.LEARNER, rewards);
        builder.publish(Component.LEARNER, actions);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, actions);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

}
