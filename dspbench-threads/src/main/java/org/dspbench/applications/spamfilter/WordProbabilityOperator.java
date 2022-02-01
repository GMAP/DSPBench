package org.dspbench.applications.spamfilter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.dspbench.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.applications.spamfilter.SpamFilterConstants.DEFAULT_WORDMAP;

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
        String wordMapFile = config.getString(SpamFilterConstants.Config.WORD_PROB_WORDMAP, null);
        boolean useDefault = config.getBoolean(SpamFilterConstants.Config.WORD_PROB_WORDMAP_USE_DEFAULT, true);

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

        LOG.info("LOADMAP ===================== " + words);
    }

    @Override
    public void process(Tuple input) {
        if (input.getStreamId().equals(SpamFilterConstants.Streams.T_TOKENS)) {
            String word = input.getString(SpamFilterConstants.Field.WORD);
            int count = input.getInt(SpamFilterConstants.Field.COUNT);
            boolean isSpam = input.getBoolean(SpamFilterConstants.Field.IS_SPAM);
            
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
        
        else if (input.getStreamId().equals(SpamFilterConstants.Streams.T_SUMS)) {
            int spamCount = input.getInt(SpamFilterConstants.Field.SPAM_TOTAL);
            int hamCount  = input.getInt(SpamFilterConstants.Field.HAM_TOTAL);
            
            //spamTotal += spamCount;
            //hamTotal  += hamCount;
            
            words.incSpamTotal(spamCount);
            words.incHamTotal(hamCount);
            
            for (Word word : words.values()) {
                //word.calcProbs(spamTotal, hamTotal);
                word.calcProbs(words.getSpamTotal(), words.getHamTotal());
            }
        }
        
        else if (input.getStreamId().equals(SpamFilterConstants.Streams.TOKENS)) {
            String emailId = input.getString(SpamFilterConstants.Field.ID);
            String word    = input.getString(SpamFilterConstants.Field.WORD);
            int numWords   = input.getInt(SpamFilterConstants.Field.NUM_WORDS);
            
            Word w = words.get(word);

            if (w == null) {
                w = new Word(word);
                w.setpSpam(0.4f);
            }
            
            emit(input, new Values(emailId, w, numWords));
        }
    }
    
    private static Kryo getKryoInstance() {
        if (kryoInstance == null) {
            kryoInstance = new Kryo();
            kryoInstance.register(Word.class, new Word.WordSerializer());
            kryoInstance.register(WordMap.class, new WordMap.WordMapSerializer());

            MapSerializer serializer = new MapSerializer();
            kryoInstance.register(HashMap.class, serializer);
            kryoInstance.register(LinkedHashMap.class, serializer);
        }
        
        return kryoInstance;
    }
    
    private static WordMap loadDefaultWordMap() {
        try {
            Input input = new Input(WordProbabilityOperator.class.getResourceAsStream(SpamFilterConstants.DEFAULT_WORDMAP));
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
}
