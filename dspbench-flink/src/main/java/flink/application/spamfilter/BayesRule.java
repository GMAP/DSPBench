package flink.application.spamfilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.SpamFilterConstants;
import flink.util.Metrics;

public class BayesRule extends Metrics implements FlatMapFunction<Tuple3<String, Word, Integer>, Tuple3<String, Float, Boolean>> {
    private static final Logger LOG = LoggerFactory.getLogger(BayesRule.class);
    Configuration config;
    
    private double spamProbability;
    private Map<String, AnalysisSummary> analysisSummary;

    public BayesRule(Configuration config){
        super.initialize(config);
        this.config = config;

        spamProbability = config.getDouble(SpamFilterConstants.Conf.BAYES_RULE_SPAM_PROB, 0.9d);
        analysisSummary = new HashMap<>();
    }

    @Override
    public void flatMap(Tuple3<String, Word, Integer> value, Collector<Tuple3<String, Float, Boolean>> out)
            throws Exception {
        super.initialize(config);
        super.incReceived();
        String id    = value.getField(0);
        Word word    = (Word) value.getField(1);
        int numWords = value.getField(2);
        
        AnalysisSummary summary = analysisSummary.get(id);
        
        if (summary == null) {
            summary = new AnalysisSummary();
            analysisSummary.put(id, summary);
        }
        
        summary.uniqueWords++;
        
        updateSummary(summary, word);
        
        if (summary.uniqueWords >= numWords) {
            // calculate bayes
            float pspam = bayes(summary);
            super.incEmitted();
            //collector.emit(new Values(id, pspam, (pspam > spamProbability)));
            out.collect(new Tuple3<String,Float,Boolean>(id, pspam, (pspam > spamProbability)));
            analysisSummary.remove(id);
        }
    }

    private float bayes(AnalysisSummary summary) {
        // Apply Bayes' rule (via Graham)
        float pposproduct = 1.0f;
        float pnegproduct = 1.0f;
        
        // For every word, multiply Spam probabilities ("Pspam") together
        // (As well as 1 - Pspam)
        for (Word w : summary) {
            pposproduct *= w.getpSpam();
            pnegproduct *= (1.0f - w.getpSpam());
        }

        // Apply formula
        return pposproduct / (pposproduct + pnegproduct);
    }
    
    private void updateSummary(AnalysisSummary summary, Word word) {
        int limit = 15;
        
        // If this list is empty, then add this word in!
        if (summary.isEmpty()) {
            summary.add(word);
        }
        
        // Otherwise, add it in sorted order by interesting level
        else {
            for (int j = 0; j < summary.size(); j++) {
                // For every word in the list already
                Word nw = summary.get(j);

                // If it's the same word, don't bother
                if (word.getWord().equals(nw.getWord())) {
                    break;
                    // If it's more interesting stick it in the list
                } else if (word.interesting() > nw.interesting()) {
                    summary.add(j, word);
                    break;
                }
                
                // If we get to the end, just tack it on there
                else if (j == summary.size()-1) {
                    summary.add(word);
                }
            }
        }

        // If the list is bigger than the limit, delete entries
        // at the end (the more "interesting" ones are at the 
        // start of the list
        while (summary.size() > limit)
            summary.remove(summary.size()-1);
    }

    private static class AnalysisSummary extends ArrayList<Word> {
        public int uniqueWords = 0;
    }
}
