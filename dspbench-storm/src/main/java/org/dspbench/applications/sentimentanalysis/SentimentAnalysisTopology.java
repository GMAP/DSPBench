package org.dspbench.applications.sentimentanalysis;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.applications.sentimentanalysis.SentimentAnalysisConstants.*;

/**
 * Orchestrates the elements and forms a Topology to find the most happiest state
 * by analyzing and processing Tweets.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class SentimentAnalysisTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisTopology.class);
    
    private int classifierThreads;
    
    public SentimentAnalysisTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        classifierThreads = config.getInt(Conf.CLASSIFIER_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.ID, Field.TWEET, Field.TIMESTAMP));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.CLASSIFIER, new CalculateSentimentBolt(), classifierThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.CLASSIFIER);
        
        return builder.createTopology();
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
