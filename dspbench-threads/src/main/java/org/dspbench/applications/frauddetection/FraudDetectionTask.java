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

package org.dspbench.applications.frauddetection;

import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.base.task.BasicTask;
import org.dspbench.utils.Configuration;
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
        
        predictorThreads = config.getInt(FraudDetectionConstants.Config.PREDICTOR_THREADS, 1);
    }
    
    public void initialize() {
        Stream transactions = builder.createStream(FraudDetectionConstants.Streams.TRANSACTIONS,
                new Schema().keys(FraudDetectionConstants.Field.ENTITY_ID).fields(FraudDetectionConstants.Field.RECORD_DATA));
        Stream predictions  = builder.createStream(FraudDetectionConstants.Streams.PREDICTIONS,
                new Schema().keys(FraudDetectionConstants.Field.ENTITY_ID).fields(FraudDetectionConstants.Field.SCORE, FraudDetectionConstants.Field.STATES));
        
        builder.setSource(FraudDetectionConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(FraudDetectionConstants.Component.SOURCE, transactions);
        builder.setTupleRate(FraudDetectionConstants.Component.SOURCE, sourceRate);
        
        builder.setOperator(FraudDetectionConstants.Component.PREDICTOR, new PredictorOperator(), predictorThreads);
        builder.groupByKey(FraudDetectionConstants.Component.PREDICTOR, transactions);
        builder.publish(FraudDetectionConstants.Component.PREDICTOR, predictions);
        
        builder.setOperator(FraudDetectionConstants.Component.SINK, sink, sinkThreads);
        builder.groupByKey(FraudDetectionConstants.Component.SINK, predictions);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return FraudDetectionConstants.PREFIX;
    }
}
