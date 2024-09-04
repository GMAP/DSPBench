package flink.application.spamfilter;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import flink.util.Configurations;
import flink.util.Metrics;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import flink.constants.SpamFilterConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TokenWordProb extends RichCoFlatMapFunction<Tuple3<String, String, Boolean>,Tuple3<String, String, Boolean>,Tuple3<String, Word, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(TokenWordProb.class);
    Configuration config;
    Metrics metrics = new Metrics();

    private WordMap wordMap;
    private static final String splitregex = "\\W";
    private static final Pattern wordregex = Pattern.compile("\\w+");
    
    public TokenWordProb(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;

        String wordMapFile = config.getString(SpamFilterConstants.Conf.WORD_PROB_WORDMAP, null);
        boolean useDefault = config.getBoolean(SpamFilterConstants.Conf.WORD_PROB_WORDMAP_USE_DEFAULT, true);

        if (wordMapFile != null) {
            wordMap = loadWordMap(wordMapFile);
        } 
        
        if (wordMap == null) {
            if (useDefault) {
                wordMap = loadDefaultWordMap();
            } else {
                wordMap = new WordMap();
            }
        }
    }

    private static WordMap loadDefaultWordMap() {
        try {
            Input input = new Input(TokenWordProb.class.getResourceAsStream(SpamFilterConstants.DEFAULT_WORDMAP));
            WordMap object = new ObjectMapper().readValue(input, WordMap.class);
            input.close();
            return object;
        } catch(IOException ex) {
            LOG.error("Unable to deserialize the wordmap object", ex);
        }
        
        return null;
    }
    
    private static WordMap loadWordMap(String path) {
        try {
            Input input = new Input(new FileInputStream(path));
            WordMap object = new ObjectMapper().readValue(input, WordMap.class);
            input.close();
            return object;
        } catch(IOException ex) {
            LOG.error("Unable to deserialize the wordmap object", ex);
        }
        
        return null;
    }

    @Override
    public void flatMap1(Tuple3<String, String, Boolean> value, Collector<Tuple3<String, Word, Integer>> out)
            throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        //TRAINING
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String content = value.getField(1);
        boolean isSpam = value.getField(2);
            
        Map<String, MutableInt> words = tokenize(content);
        int spamTotal = 0, hamTotal = 0;

        for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
            String word = entry.getKey();
            int count = entry.getValue().toInteger();
            
            if (isSpam) {
                spamTotal += count;
            } else {
                hamTotal += count;
            }

            //collector.emit(Stream.TRAINING, input, new Values(word, count, isSpam));     
            Word w = wordMap.get(word);
            
            if (w == null) {
                w = new Word(word);
                wordMap.put(word, w);
            }

            if (isSpam) {
                w.countBad(count);
            } else {
                w.countGood(count);
            }
        }

        //collector.emit(Stream.TRAINING_SUM, input, new Values(spamTotal, hamTotal));
        wordMap.incSpamTotal(spamTotal);
        wordMap.incHamTotal(hamTotal);
        
        for (Word word : wordMap.values()) {
            word.calcProbs(wordMap.getSpamTotal(), wordMap.getHamTotal());
        }
    }

    @Override
    public void flatMap2(Tuple3<String, String, Boolean> value, Collector<Tuple3<String, Word, Integer>> out)
            throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        //ANALYSIS
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String id = value.getField(0);
        String content = value.getField(1);
    
        Map<String, MutableInt> words = tokenize(content);
        
        for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
            //collector.emit(Stream.ANALYSIS, input, new Values(id, entry.getKey(), words.size()));
            Word w = wordMap.get(entry.getKey());

            if (w == null) {
                w = new Word(entry.getKey());
                w.setpSpam(0.4f);
            }
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            out.collect(new Tuple3<String,Word,Integer>(id, w, words.size()));
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    private Map<String, MutableInt> tokenize(String content) {
        String[] tokens = content.split(splitregex);
        Map<String, MutableInt> words = new HashMap<>();

        for (String token : tokens) {
            String word = token.toLowerCase();
            Matcher m = wordregex.matcher(word);

            if (m.matches()) {
                MutableInt count = words.get(word);
                if (count == null) {
                    words.put(word, new MutableInt());
                } else {
                    count.increment();
                }
            }
        }
        
        return words;
    }
    
}