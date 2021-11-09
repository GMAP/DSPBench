package com.streamer.examples.spamfilter;

import com.streamer.base.constants.BaseConstants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface SpamFilterConstants extends BaseConstants {
    String PREFIX = "sf";
    String DEFAULT_WORDMAP = "/spamfilter/wordmap.bin";
    
    interface Config extends BaseConfig {
        String PARSER_THREADS     = "sf.parser.threads";
        String TOKENIZER_THREADS  = "sf.tokenizer.threads";
        String WORD_PROB_THREADS  = "sf.wordprob.threads";
        String BAYES_RULE_THREADS = "sf.bayesrule.threads";
        String BAYES_RULE_SPAM_PROB = "sf.bayesrule.spam_probability";
        String WORD_PROB_WORDMAP  = "sf.wordprob.wordmap";
        String WORD_PROB_WORDMAP_USE_DEFAULT  = "sf.wordprob.wordmap.use_default";
    }
    
    interface Field {
        String ID = "id";
        String TYPE = "type";
        String MESSAGE = "message";
        String IS_SPAM = "isSpam";
        String WORD = "word";
        String WORD_OBJ = "wordObject";
        String NUM_WORDS = "numWords";
        String COUNT = "count";
        String SPAM_TOTAL = "spamTotal";
        String HAM_TOTAL = "hamTotal";
        String SPAM_PROB = "spamProb";
    }
    
    interface Streams {
        String T_EMAILS = "trainingEmailStream";
        String T_TOKENS = "trainingTokenStream";
        String T_SUMS   = "trainingSumStream";
        
        String EMAILS   = "emailStream";
        String TOKENS   = "tokenStream";
        String PROBS    = "probabilitieStream";
        String RESULTS  = "resultStream";
    }
    
    interface Component extends BaseComponent {
        String TRAINING_SPOUT = "trainingSpout";
        String ANALYSIS_SPOUT = "analysisSpout";
        String PARSER = "parserBolt";
        String TOKENIZER = "tokenizerBolt";
        String WORD_PROBABILITY = "wordProbabilityBolt";
        String BAYES_RULE = "bayesRuleBolt";
    }
}
