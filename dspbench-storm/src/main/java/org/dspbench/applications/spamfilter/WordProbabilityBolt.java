package org.dspbench.applications.spamfilter;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.dspbench.bolt.AbstractBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.applications.spamfilter.SpamFilterConstants.Conf;
import static org.dspbench.applications.spamfilter.SpamFilterConstants.DEFAULT_WORDMAP;
import org.dspbench.applications.spamfilter.SpamFilterConstants.Field;
import org.dspbench.applications.spamfilter.SpamFilterConstants.Stream;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class WordProbabilityBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordProbabilityBolt.class);
    private static Kryo kryoInstance;
    
    private WordMap words;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.WORD, Field.NUM_WORDS);
    }

    @Override
    public void initialize() {
        String wordMapFile = config.getString(Conf.WORD_PROB_WORDMAP, null);
        boolean useDefault = config.getBoolean(Conf.WORD_PROB_WORDMAP_USE_DEFAULT, true);

        LOG.info("INITIALIZE WORD PROBABILITY ========================================================================");
        
        if (wordMapFile != null) {
            LOG.info("INITIALIZE || LOAD WORD MAP ========================================================================");
            words = loadWordMap(wordMapFile);
        } 
        
        if (words == null) {
            if (useDefault) {
                LOG.info("INITIALIZE LOAD DEFAULT WORD MAP ========================================================================");
                words = loadDefaultWordMap();
            } else {
                LOG.info("INITIALIZE LOAD NEW WORD MAP ========================================================================");
                words = new WordMap();
            }
        }
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Stream.TRAINING)) {
            String word = input.getStringByField(Field.WORD);
            int count = input.getIntegerByField(Field.COUNT);
            boolean isSpam = input.getBooleanByField(Field.IS_SPAM);
            
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
        
        else if (input.getSourceStreamId().equals(Stream.TRAINING_SUM)) {
            int spamCount = input.getIntegerByField(Field.SPAM_TOTAL);
            int hamCount  = input.getIntegerByField(Field.HAM_TOTAL);
            
            words.incSpamTotal(spamCount);
            words.incHamTotal(hamCount);
            
            for (Word word : words.values()) {
                word.calcProbs(words.getSpamTotal(), words.getHamTotal());
            }
        }
        
        else if (input.getSourceStreamId().equals(Stream.ANALYSIS)) {
            String id = input.getStringByField(Field.ID);
            String word = input.getStringByField(Field.WORD);
            int numWords = input.getIntegerByField(Field.NUM_WORDS);
            
            Word w = words.get(word);

            if (w == null) {
                w = new Word(word);
                w.setPSpam(0.4f);
            }
            
            collector.emit(input, new Values(id, w, numWords));
        }
        
        collector.ack(input);
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
            LOG.info("======================= LOAD_MAP");
            Input input = new Input(WordProbabilityBolt.class.getResourceAsStream(DEFAULT_WORDMAP));
            WordMap object = getKryoInstance().readObject(input, WordMap.class);
            input.close();

            LOG.info("======================= MAP_LOADED = " + object);
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
