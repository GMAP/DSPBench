package com.streamer.examples.spamfilter.train;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.Files;
import com.streamer.examples.spamfilter.Word;
import com.streamer.examples.spamfilter.WordMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author mayconbordin
 */
public class OfflineTraining {
    // How to split the String into  tokens
    private static final String splitregex = "\\W";
    // Regex to eliminate junk (although we really should welcome the junk)
    private static final Pattern wordregex = Pattern.compile("\\w+");
    
    private static Kryo kryoInstance;

    // A HashMap to keep track of all words
    protected WordMap words;

    public OfflineTraining() {
        // Initialize fields
        words = new WordMap();
    }
    
    public void train(String content, boolean isSpam) {
        String[] tokens = content.split(splitregex);
        
        for (String token : tokens) {
            String word = token.toLowerCase();
            Matcher m = wordregex.matcher(word);
            
            if (m.matches()) {
                Word w;
                
                if (words.containsKey(word)) {
                    w = words.get(word);
                } else {
                    w = new Word(word);
                    words.put(word, w);
                }
                
                if (isSpam) {
                    w.countBad();
                    words.incSpamTotal(1);
                } else {
                    w.countGood();
                    words.incHamTotal(1);
                }
            }
        }
    }
    
    public void finalizeTraining() {
        for (Word word : words.values()) {
            word.calcBadProb(words.getSpamTotal());
            word.calcGoodProb(words.getHamTotal());
            word.finalizeProb();
        }
    }
    
    public boolean saveTraining(String filePath) {
        try {
            Output output = new Output(new FileOutputStream(filePath));
            getKryoInstance().writeObject(output, words);
            output.close();
            return true;
        } catch(FileNotFoundException ex) {
            System.out.println("The file path was not found");
            ex.printStackTrace();
        } catch(KryoException ex) {
            System.out.println("Serialization error");
            ex.printStackTrace();
        }
        
        return false;
    }
    
    public boolean loadTraining(String filePath) {
        try {
            Input input = new Input(new FileInputStream(filePath));
            WordMap object = getKryoInstance().readObject(input, WordMap.class);
            input.close();
            words = object;
            return true;
        } catch(FileNotFoundException ex) {
            System.out.println("The file path was not found");
            ex.printStackTrace();
        } catch(KryoException ex) {
            System.out.println("Deserialization error");
            ex.printStackTrace();
        }
        
        return false;
    }
    
    private static Kryo getKryoInstance() {
        if (kryoInstance == null) {
            kryoInstance = new Kryo();
            kryoInstance.register(Word.class, new Word.WordSerializer());
            kryoInstance.register(WordMap.class, new WordMap.WordMapSerializer());
        }
        
        return kryoInstance;
    }
    
    public static void main(String args[]) throws IOException {
        /*
        String path = "/home/mayconbordin/Projects/datasets/spam/trec07p";
        OfflineTraining filter = new OfflineTraining();
        List<String> trainingSet = Files.readLines(new File(path + "/full/index"), Charset.defaultCharset());
        
        System.out.println("Number of emails: " + trainingSet.size());
        
        for (int i=0; i<trainingSet.size(); i++) {
            if (i%1000 == 0)
                System.out.println("Training set " + i);
            
            String[] train = trainingSet.get(i).split("\\s+");
            
            boolean isSpam = train[0].toLowerCase().trim().equals("spam");
            String content = Files.toString(new File(path + "/data/" + train[1]), Charset.defaultCharset());
            
            filter.train(content, isSpam);
        }
        
        filter.finalizeTraining();
        
        boolean result = filter.saveTraining("/home/mayconbordin/Projects/datasets/spam/full_training.bin");
        
        if (!result) {
            System.out.println("Not saved");
        } else {
            System.out.println("Saved");
        }*/
        
        
        OfflineTraining filter = new OfflineTraining();
        filter.loadTraining("/home/mayconbordin/Projects/datasets/spam/full_training.bin");
        
        System.out.println("Num words: " + filter.words.values().size());
    }
}
