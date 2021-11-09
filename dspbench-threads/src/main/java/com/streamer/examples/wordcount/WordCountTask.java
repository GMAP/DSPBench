package com.streamer.examples.wordcount;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.base.task.AbstractTask;
import com.streamer.base.task.BasicTask;
import static com.streamer.examples.wordcount.WordCountConstants.*;
import com.streamer.utils.Configuration;
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
        
        splitterThreads = config.getInt(Config.SPLITTER_THREADS, 1);
        counterThreads  = config.getInt(Config.COUNTER_THREADS, 1);
    }

    public void initialize() {
        Stream sentences = builder.createStream(Streams.SENTENCES, new Schema(Field.TEXT));
        Stream words     = builder.createStream(Streams.WORDS, new Schema().keys(Field.WORD));
        Stream counts    = builder.createStream(Streams.COUNTS, new Schema().keys(Field.WORD).fields(Field.COUNT));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, sentences);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.SPLITTER, new SplitSentenceOperator(), splitterThreads);
        builder.publish(Component.SPLITTER, words);
        builder.shuffle(Component.SPLITTER, sentences);

        builder.setOperator(Component.COUNTER, new WordCountOperator(), counterThreads);
        builder.publish(Component.COUNTER, counts);
        builder.groupByKey(Component.COUNTER, words);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, counts);
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
