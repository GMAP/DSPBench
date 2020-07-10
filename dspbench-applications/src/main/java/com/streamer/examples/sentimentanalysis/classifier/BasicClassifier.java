package com.streamer.examples.sentimentanalysis.classifier;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.streamer.examples.sentimentanalysis.SentimentAnalysisConstants.Config;
import com.streamer.utils.Configuration;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicClassifier implements SentimentClassifier {
    private static final Logger LOG = LoggerFactory.getLogger(BasicClassifier.class);
    private static final String DEFAULT_PATH = "sentimentanalysis/AFINN-111.txt";
    private SortedMap<String,Integer> afinnSentimentMap;
    
    public void initialize(Configuration config) {
        afinnSentimentMap = Maps.newTreeMap();
        
        try {
            final URL url = Resources.getResource(config.getString(Config.BASIC_CLASSIFIER_PATH, DEFAULT_PATH));
            final String text = Resources.toString(url, Charsets.UTF_8);
            final Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
            List<String> tabSplit;
            
            for (final String str: lineSplit) {
                tabSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
                afinnSentimentMap.put(tabSplit.get(0), Integer.parseInt(tabSplit.get(1)));
            }
        } catch (final IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Unable to read the affinity file.", ex);
        }
    }
    
    

    public SentimentResult classify(String str) {
        //Remove all punctuation and new line chars in the tweet.
        final String text = str.replaceAll("\\p{Punct}|\\n", " ").toLowerCase();
        
        //Splitting the tweet on empty space.
        final Iterable<String> words = Splitter.on(' ')
                                               .trimResults()
                                               .omitEmptyStrings()
                                               .split(text);
        int sentimentOfCurrentTweet = 0;
        
        //Loop thru all the words and find the sentiment of this text
        for (final String word : words) {
            if (afinnSentimentMap.containsKey(word)){
                sentimentOfCurrentTweet += afinnSentimentMap.get(word);
            }
        }
        
        SentimentResult result = new SentimentResult();
        result.setScore(sentimentOfCurrentTweet);
        
        if (sentimentOfCurrentTweet > 0)
            result.setSentiment(SentimentResult.Sentiment.Positive);
        else if (sentimentOfCurrentTweet < 0)
            result.setSentiment(SentimentResult.Sentiment.Negative);
        else
            result.setSentiment(SentimentResult.Sentiment.Neutral);
        
        return result;
    }
    
}
