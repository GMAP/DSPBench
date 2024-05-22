package flink.application.spamfilter;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import flink.util.Configurations;
import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import flink.constants.BaseConstants;
import flink.constants.SpamFilterConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.util.MetricsFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.esotericsoftware.kryo.io.Input;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TokenWordProb extends RichCoFlatMapFunction<Tuple3<String, String, Boolean>,Tuple3<String, String, Boolean>,Tuple3<String, Word, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(TokenWordProb.class);
    Configuration config;
    Metric metrics = new Metric();

    private WordMap wordMap;
    private static final String splitregex = "\\W";
    private static final Pattern wordregex = Pattern.compile("\\w+");
    
    public TokenWordProb(Configuration config){
        metrics.initialize(config);
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
        //TRAINNING
        metrics.incReceived("TokenWordProb");
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
        //ANALYSIS
        metrics.incReceived("TokenWordProb");
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
            metrics.incEmitted("TokenWordProb");
            out.collect(new Tuple3<String,Word,Integer>(id, w, words.size()));
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

class Metric implements Serializable {
    Configuration config;
    private final Map<String, Long> throughput = new HashMap<>();
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(150);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(Metric.class);

    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;

    public void initialize(Configuration config) {
        this.config = config;
        getMetrics();
        File pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK")).toFile();

        pathTrh.mkdirs();

        this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
    }

    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    System.out.println("Error while writing the file " + file + " - " + ex);
                }
            } catch (Exception e) {
                System.out.println("Error while creating the file " + e.getMessage());
            }
        }).start();
    }

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived(String name) {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(name + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted(String name) {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(name + "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived(String name) {
        getTuplesReceived(name).inc();
    }

    protected void incReceived(String name, long n) {
        getTuplesReceived(name).inc(n);
    }

    protected void incEmitted(String name) {
        getTuplesEmitted(name).inc();
    }

    protected void incEmitted(String name, long n) {
        getTuplesEmitted(name).inc(n);
    }

    protected void incBoth(String name) {
        getTuplesReceived(name).inc();
        getTuplesEmitted(name).inc();
    }
}