package com.streamer.examples.spamfilter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import static com.streamer.examples.spamfilter.SpamFilterConstants.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class WordProbabilityOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(WordProbabilityOperator.class);
    
    private static Kryo kryoInstance;

    private WordMap words;

    @Override
    public void initialize() {
        String wordMapFile = config.getString(Config.WORD_PROB_WORDMAP, null);
        boolean useDefault = config.getBoolean(Config.WORD_PROB_WORDMAP_USE_DEFAULT, true);
        
        if (wordMapFile != null) {
            words = loadWordMap(wordMapFile);
        } 
        
        if (words == null) {
            if (useDefault) {
                words = loadDefaultWordMap();
            } else {
                words = new WordMap();
            }
        }
    }

    @Override
    public void process(Tuple input) {
        if (input.getStreamId().equals(Streams.T_TOKENS)) {
            String word = input.getString(Field.WORD);
            int count = input.getInt(Field.COUNT);
            boolean isSpam = input.getBoolean(Field.IS_SPAM);
            
            Word w = words.get(word);
            
            if (w == null) {
                w = new Word(word);
                words.put(word, w);
            }

            if (isSpam) {
                w.countBad(count);
            } else {
                w.countGood(count);
            }
        }
        
        else if (input.getStreamId().equals(Streams.T_SUMS)) {
            int spamCount = input.getInt(Field.SPAM_TOTAL);
            int hamCount  = input.getInt(Field.HAM_TOTAL);
            
            //spamTotal += spamCount;
            //hamTotal  += hamCount;
            
            words.incSpamTotal(spamCount);
            words.incHamTotal(hamCount);
            
            for (Word word : words.values()) {
                //word.calcProbs(spamTotal, hamTotal);
                word.calcProbs(words.getSpamTotal(), words.getHamTotal());
            }
        }
        
        else if (input.getStreamId().equals(Streams.TOKENS)) {
            String emailId = input.getString(Field.ID);
            String word    = input.getString(Field.WORD);
            int numWords   = input.getInt(Field.NUM_WORDS);
            
            Word w = words.get(word);

            if (w == null) {
                w = new Word(word);
                w.setPSpam(0.4f);
            }
            
            emit(input, new Values(emailId, w, numWords));
        }
    }
    
    private static Kryo getKryoInstance() {
        if (kryoInstance == null) {
            kryoInstance = new Kryo();
            kryoInstance.register(Word.class, new Word.WordSerializer());
            kryoInstance.register(WordMap.class, new WordMap.WordMapSerializer());
        }
        
        return kryoInstance;
    }
    
    private static WordMap loadDefaultWordMap() {
        try {
            Input input = new Input(WordProbabilityOperator.class.getResourceAsStream(DEFAULT_WORDMAP));
            WordMap object = getKryoInstance().readObject(input, WordMap.class);
            input.close();
            return object;
        } catch(KryoException ex) {
            LOG.error("Unable to deserialize the wordmap object", ex);
        }
        
        return null;
    }
    
    private static WordMap loadWordMap(String path) {
        try {
            Input input = new Input(new FileInputStream(path));
            WordMap object = getKryoInstance().readObject(input, WordMap.class);
            input.close();
            return object;
        } catch(FileNotFoundException ex) {
            LOG.error("The file path was not found", ex);
        } catch(KryoException ex) {
            LOG.error("Unable to deserialize the wordmap object", ex);
        }
        
        return null;
    }
}
