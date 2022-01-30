package org.dspbench.applications.wordcount;

import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.base.task.BasicTask;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class WordCountTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTask.class);
    
    private int splitterThreads;
    private int counterThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        splitterThreads = config.getInt(WordCountConstants.Config.SPLITTER_THREADS, 1);
        counterThreads  = config.getInt(WordCountConstants.Config.COUNTER_THREADS, 1);
    }

    public void initialize() {
        Stream sentences = builder.createStream(WordCountConstants.Streams.SENTENCES, new Schema(WordCountConstants.Field.TEXT));
        Stream words     = builder.createStream(WordCountConstants.Streams.WORDS, new Schema().keys(WordCountConstants.Field.WORD));
        Stream counts    = builder.createStream(WordCountConstants.Streams.COUNTS, new Schema().keys(WordCountConstants.Field.WORD).fields(WordCountConstants.Field.COUNT));
        
        builder.setSource(WordCountConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(WordCountConstants.Component.SOURCE, sentences);
        builder.setTupleRate(WordCountConstants.Component.SOURCE, sourceRate);
        
        builder.setOperator(WordCountConstants.Component.SPLITTER, new SplitSentenceOperator(), splitterThreads);
        builder.publish(WordCountConstants.Component.SPLITTER, words);
        builder.shuffle(WordCountConstants.Component.SPLITTER, sentences);

        builder.setOperator(WordCountConstants.Component.COUNTER, new WordCountOperator(), counterThreads);
        builder.publish(WordCountConstants.Component.COUNTER, counts);
        builder.groupByKey(WordCountConstants.Component.COUNTER, words);
        
        builder.setOperator(WordCountConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(WordCountConstants.Component.SINK, counts);
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
