package org.dspbench.applications.wordcount;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    private int splitSentenceThreads;
    private int wordCountThreads;

    public WordCountTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        splitSentenceThreads = config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1);
        wordCountThreads     = config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(WordCountConstants.Field.TEXT));
        
        builder.setSpout(WordCountConstants.Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(WordCountConstants.Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads)
               .shuffleGrouping(WordCountConstants.Component.SPOUT);
        
        builder.setBolt(WordCountConstants.Component.COUNTER, new WordCountBolt(), wordCountThreads)
               .fieldsGrouping(WordCountConstants.Component.SPLITTER, new Fields(WordCountConstants.Field.WORD));
        
        builder.setBolt(WordCountConstants.Component.SINK, sink, sinkThreads)
               .shuffleGrouping(WordCountConstants.Component.COUNTER);
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return WordCountConstants.PREFIX;
    }
    
}
