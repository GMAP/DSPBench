package org.dspbench.applications.spamfilter;

import org.dspbench.base.sink.BaseSink;
import org.dspbench.base.source.BaseSource;
import org.dspbench.base.task.AbstractTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.partitioning.Fields;
import org.dspbench.utils.Configuration;
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
        super.setConfiguration(config);

        trainingSource = loadSource("training");
        analysisSource = loadSource("analysis");
        sink = loadSink();
        
        trainingSourceThreads = config.getInt(getConfigKey(SpamFilterConstants.Config.SOURCE_THREADS, "training"), 1);
        analysisSourceThreads = config.getInt(getConfigKey(SpamFilterConstants.Config.SOURCE_THREADS, "analysis"), 1);
        tokenizerThreads      = config.getInt(SpamFilterConstants.Config.TOKENIZER_THREADS, 1);
        wordProbThreads       = config.getInt(SpamFilterConstants.Config.WORD_PROB_THREADS, 1);
        bayesRuleThreads      = config.getInt(SpamFilterConstants.Config.BAYES_RULE_THREADS, 1);
        sinkThreads           = config.getInt(getConfigKey(SpamFilterConstants.Config.SINK_THREADS), 1);
    }

    public void initialize() {
        Stream trainingEmails = builder.createStream(SpamFilterConstants.Streams.T_EMAILS, new Schema().keys(SpamFilterConstants.Field.ID).fields(SpamFilterConstants.Field.MESSAGE, SpamFilterConstants.Field.IS_SPAM));
        Stream trainingTokens = builder.createStream(SpamFilterConstants.Streams.T_TOKENS, new Schema().keys(SpamFilterConstants.Field.WORD).fields(SpamFilterConstants.Field.COUNT, SpamFilterConstants.Field.IS_SPAM));
        Stream trainingSum    = builder.createStream(SpamFilterConstants.Streams.T_SUMS, new Schema(SpamFilterConstants.Field.SPAM_TOTAL, SpamFilterConstants.Field.HAM_TOTAL));
        
        Stream emails   = builder.createStream(SpamFilterConstants.Streams.EMAILS, new Schema().keys(SpamFilterConstants.Field.ID).fields(SpamFilterConstants.Field.MESSAGE));
        Stream tokens   = builder.createStream(SpamFilterConstants.Streams.TOKENS, new Schema().keys(SpamFilterConstants.Field.ID).fields(SpamFilterConstants.Field.WORD, SpamFilterConstants.Field.NUM_WORDS));
        Stream probs    = builder.createStream(SpamFilterConstants.Streams.PROBS, new Schema().keys(SpamFilterConstants.Field.ID).fields(SpamFilterConstants.Field.WORD_OBJ, SpamFilterConstants.Field.NUM_WORDS));
        Stream results  = builder.createStream(SpamFilterConstants.Streams.RESULTS, new Schema().keys(SpamFilterConstants.Field.ID).fields(SpamFilterConstants.Field.SPAM_PROB, SpamFilterConstants.Field.IS_SPAM));

        builder.setSource(SpamFilterConstants.Component.TRAINING_SPOUT, trainingSource, trainingSourceThreads);
        builder.publish(SpamFilterConstants.Component.TRAINING_SPOUT, trainingEmails);
        
        builder.setSource(SpamFilterConstants.Component.ANALYSIS_SPOUT, analysisSource, analysisSourceThreads);
        builder.publish(SpamFilterConstants.Component.ANALYSIS_SPOUT, emails);
        
        builder.setOperator(SpamFilterConstants.Component.TOKENIZER, new TokenizerOperator(), tokenizerThreads);
        builder.shuffle(SpamFilterConstants.Component.TOKENIZER, emails);
        builder.shuffle(SpamFilterConstants.Component.TOKENIZER, trainingEmails);
        builder.publish(SpamFilterConstants.Component.TOKENIZER, tokens, trainingTokens, trainingSum);
        
        builder.setOperator(SpamFilterConstants.Component.WORD_PROBABILITY, new WordProbabilityOperator(), wordProbThreads);
        builder.groupBy(SpamFilterConstants.Component.WORD_PROBABILITY, tokens, new Fields(SpamFilterConstants.Field.WORD));
        builder.groupByKey(SpamFilterConstants.Component.WORD_PROBABILITY, trainingTokens);
        builder.bcast(SpamFilterConstants.Component.WORD_PROBABILITY, trainingSum);
        builder.publish(SpamFilterConstants.Component.WORD_PROBABILITY, probs);
        
        builder.setOperator(SpamFilterConstants.Component.BAYES_RULE, new BayesRuleOperator(), bayesRuleThreads);
        builder.groupByKey(SpamFilterConstants.Component.BAYES_RULE, probs);
        builder.publish(SpamFilterConstants.Component.BAYES_RULE, results);
        
        
        builder.setOperator(SpamFilterConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(SpamFilterConstants.Component.SINK, results);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return SpamFilterConstants.PREFIX;
    }

}
