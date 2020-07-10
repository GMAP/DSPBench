package com.streamer.examples.spamfilter;

import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.sink.BaseSink;
import com.streamer.base.source.BaseSource;
import com.streamer.base.task.AbstractTask;
import com.streamer.core.Operator;
import com.streamer.core.Schema;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.core.Task;
import static com.streamer.examples.spamfilter.SpamFilterConstants.*;
import com.streamer.utils.ClassLoaderUtils;
import com.streamer.partitioning.Fields;
import com.streamer.topology.Topology;
import com.streamer.topology.TopologyBuilder;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SpamFilterTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(SpamFilterTask.class);
        
    private int trainingSourceThreads;
    private int analysisSourceThreads;
    private int sinkThreads;
    private int tokenizerThreads;
    private int wordProbThreads;
    private int bayesRuleThreads;
    
    private BaseSource trainingSource;
    private BaseSource analysisSource;
    private BaseSink sink;
    
    @Override
    public void setConfiguration(Configuration config) {
        trainingSource = loadSource("training");
        analysisSource = loadSource("analysis");
        sink = loadSink();
        
        trainingSourceThreads = config.getInt(getConfigKey(Config.SOURCE_THREADS, "training"), 1);
        analysisSourceThreads = config.getInt(getConfigKey(Config.SOURCE_THREADS, "analysis"), 1);
        tokenizerThreads      = config.getInt(Config.TOKENIZER_THREADS, 1);
        wordProbThreads       = config.getInt(Config.WORD_PROB_THREADS, 1);
        bayesRuleThreads      = config.getInt(Config.BAYES_RULE_THREADS, 1);
        sinkThreads           = config.getInt(getConfigKey(Config.SINK_THREADS), 1);
    }

    public void initialize() {
        Stream trainingEmails = builder.createStream(Streams.T_EMAILS, new Schema().keys(Field.ID).fields(Field.MESSAGE, Field.IS_SPAM));
        Stream trainingTokens = builder.createStream(Streams.T_TOKENS, new Schema().keys(Field.WORD).fields(Field.COUNT, Field.IS_SPAM));
        Stream trainingSum    = builder.createStream(Streams.T_SUMS, new Schema(Field.SPAM_TOTAL, Field.HAM_TOTAL));
        
        Stream emails   = builder.createStream(Streams.EMAILS, new Schema().keys(Field.ID).fields(Field.MESSAGE));
        Stream tokens   = builder.createStream(Streams.TOKENS, new Schema().keys(Field.ID).fields(Field.WORD, Field.NUM_WORDS));
        Stream probs    = builder.createStream(Streams.PROBS, new Schema().keys(Field.ID).fields(Field.WORD_OBJ, Field.NUM_WORDS));
        Stream results  = builder.createStream(Streams.RESULTS, new Schema().keys(Field.ID).fields(Field.SPAM_PROB, Field.IS_SPAM));

        builder.setSource(Component.TRAINING_SPOUT, trainingSource, trainingSourceThreads);
        builder.publish(Component.TRAINING_SPOUT, trainingEmails);
        
        builder.setSource(Component.ANALYSIS_SPOUT, analysisSource, analysisSourceThreads);
        builder.publish(Component.ANALYSIS_SPOUT, emails);
        
        builder.setOperator(Component.TOKENIZER, new TokenizerOperator(), tokenizerThreads);
        builder.shuffle(Component.TOKENIZER, emails);
        builder.shuffle(Component.TOKENIZER, trainingEmails);
        builder.publish(Component.TOKENIZER, tokens, trainingTokens, trainingSum);
        
        builder.setOperator(Component.WORD_PROBABILITY, new WordProbabilityOperator(), wordProbThreads);
        builder.groupBy(Component.WORD_PROBABILITY, tokens, new Fields(Field.WORD));
        builder.groupByKey(Component.WORD_PROBABILITY, trainingTokens);
        builder.bcast(Component.WORD_PROBABILITY, trainingSum);
        builder.publish(Component.WORD_PROBABILITY, probs);       
        
        builder.setOperator(Component.BAYES_RULE, new BayesRuleOperator(), bayesRuleThreads);
        builder.groupByKey(Component.BAYES_RULE, probs);
        builder.publish(Component.BAYES_RULE, results);
        
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, results);
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
