package org.dspbench.applications.spamfilter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author mayconbordin
 */
public class WordMap {
    @JsonProperty("words")
    private Map<String, Word> words;
    @JsonProperty("spam_total")
    private long spamTotal = 0;
    @JsonProperty("ham_total")
    private long hamTotal  = 0;

    public WordMap() {
        words = new HashMap<String, Word>();
    }

    public WordMap(Map<String, Word> words, long spamTotal, long hamTotal) {
        this.spamTotal = spamTotal;
        this.hamTotal = hamTotal;
        this.words = words;
    }

    public Map<String, Word> getWords() {
        return words;
    }

    public void setWords(Map<String, Word> words) {
        this.words = words;
    }

    public void setSpamTotal(long spamTotal) {
        this.spamTotal = spamTotal;
    }

    public void setHamTotal(long hamTotal) {
        this.hamTotal = hamTotal;
    }

    @JsonIgnore
    public long getSpamTotal() {
        return spamTotal;
    }

    @JsonIgnore
    public long getHamTotal() {
        return hamTotal;
    }
    
    public void incSpamTotal(long count) {
        spamTotal += count;
    }
    
    public void incHamTotal(long count) {
        hamTotal += count;
    }
    
    public void put(String key, Word w) {
        words.put(key, w);
    }
    
    public Word get(String key) {
        return words.get(key);
    }
    
    public boolean containsKey(String key) {
        return words.containsKey(key);
    }
    
    public Collection<Word> values() {
        return words.values();
    }
    
    public static class WordMapSerializer extends Serializer<WordMap> {
        public void write (Kryo kryo, Output output, WordMap object) {
            kryo.writeObject(output, object.words);
            output.writeLong(object.spamTotal);
            output.writeLong(object.hamTotal);
        }

        public WordMap read (Kryo kryo, Input input, Class<? extends WordMap> type) {
            return new WordMap(kryo.readObject(input, HashMap.class), input.readLong(), input.readLong());
        }
    }
}
