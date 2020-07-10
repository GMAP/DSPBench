/*
 * beymani: Outlier and anamoly detection 
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

package com.streamer.examples.frauddetection;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.base.task.BasicTask;
import static com.streamer.examples.frauddetection.FraudDetectionConstants.*;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm topolgy driver for outlier detection
 * @author pranab
 */
public class FraudDetectionTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionTask.class);
    
    private int predictorThreads;
    
    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        predictorThreads = config.getInt(Config.PREDICTOR_THREADS, 1);
    }
    
    public void initialize() {
        Stream transactions = builder.createStream(Streams.TRANSACTIONS,
                new Schema().keys(Field.ENTITY_ID).fields(Field.RECORD_DATA));
        Stream predictions  = builder.createStream(Streams.PREDICTIONS,
                new Schema().keys(Field.ENTITY_ID).fields(Field.SCORE, Field.STATES));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, transactions);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.PREDICTOR, new PredictorOperator(), predictorThreads);
        builder.groupByKey(Component.PREDICTOR, transactions);
        builder.publish(Component.PREDICTOR, predictions);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.groupByKey(Component.SINK, predictions);
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
