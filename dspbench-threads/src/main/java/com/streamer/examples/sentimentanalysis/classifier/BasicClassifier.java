package com.streamer.examples.sentimentanalysis.classifier;

import com.streamer.examples.sentimentanalysis.SentimentAnalysisConstants.Config;
import com.streamer.utils.Configuration;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.streamer.utils.ResourceUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicClassifier implements SentimentClassifier {
    private static final Logger LOG = LoggerFactory.getLogger(BasicClassifier.class);
    private static final String DEFAULT_PATH = "sentimentanalysis/AFINN-111.txt";
    private SortedMap<String,Integer> afinnSentimentMap;
    
    public void initialize(Configuration config) {
        afinnSentimentMap = new TreeMap<>();

        try {
            final String text = ResourceUtils.getResourceFileAsString(config.getString(Config.BASIC_CLASSIFIER_PATH, DEFAULT_PATH));
            final Iterable<String> lineSplit = Arrays.stream(text.split("\n")).map(String::trim).filter(StringUtils::isNotEmpty).collect(Collectors.toList());
            List<String> tabSplit;
            
            for (final String str: lineSplit) {
                tabSplit = Arrays.stream(str.split("\t")).map(String::trim).filter(StringUtils::isNotEmpty).collect(Collectors.toList());
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
        final Iterable<String> words = Arrays.stream(text.split(" ")).map(String::trim).filter(StringUtils::isNotEmpty).collect(Collectors.toList());

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
