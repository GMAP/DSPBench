package com.streamer.examples.spamfilter;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.spamfilter.SpamFilterConstants.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BayesRuleOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(BayesRuleOperator.class);
    private static final float SPAM_PROB = 0.9f;
    
    private Map<String, AnalysisSummary> analysisSummary;

    @Override
    public void initialize() {
        analysisSummary = new HashMap<String, AnalysisSummary>();
    }

    public void process(Tuple input) {
        String emailId = input.getString(Field.ID);
        Word word      = (Word) input.getValue(Field.WORD_OBJ);
        int numWords   = input.getInt(Field.NUM_WORDS);

        AnalysisSummary summary = analysisSummary.get(emailId);
                
        if (summary == null) {
            summary = new AnalysisSummary();
            summary.first = input;
            analysisSummary.put(emailId, summary);
        }
        
        summary.uniqueWords++;
        
        updateSummary(summary, word);
        
        if (summary.uniqueWords >= numWords) {
            // calculate bayes
            float pspam = bayes(summary);
            
            analysisSummary.remove(emailId);
            emit(summary.first, new Values(emailId, pspam, (pspam > SPAM_PROB)));
        }
    }
    
    private float bayes(AnalysisSummary summary) {
        // Apply Bayes' rule (via Graham)
        float pposproduct = 1.0f;
        float pnegproduct = 1.0f;
        
        // For every word, multiply Spam probabilities ("Pspam") together
        // (As well as 1 - Pspam)
        for (Word w : summary) {
            pposproduct *= w.getPSpam();
            pnegproduct *= (1.0f - w.getPSpam());
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
        while (summary.size() > limit) {
            summary.remove(summary.size()-1);
        }
    }
    
    private static class AnalysisSummary extends ArrayList<Word> {
        public int uniqueWords = 0;
        public Tuple first;
    }
}
