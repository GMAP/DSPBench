package org.dspbench.applications.spamfilter.train;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.dspbench.applications.spamfilter.Word;
import org.dspbench.applications.spamfilter.WordMap;

import java.io.*;
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

    public boolean saveTrainingAsString(String filePath) {
        try {
            String json = new ObjectMapper().writeValueAsString(words);
            FileUtils.writeStringToFile(new File(filePath), json);
            return true;
        } catch(IOException ex) {
            System.out.println("Failed to save JSON file: " + ex.getMessage());
            ex.printStackTrace();
        }

        return false;
    }

    public boolean loadTrainingString(String filePath) {
        try {
            InputStream input = new FileInputStream(filePath);
            WordMap object = new ObjectMapper().readValue(input, WordMap.class);
            input.close();
            words = object;
            return true;
        } catch(IOException ex) {
            System.out.println("Failed to read JSON file: " + ex.getMessage());
            ex.printStackTrace();
        }

        return false;
    }
    
    public static void main(String args[]) throws IOException {
//        String path = "/home/mayconbordin/Downloads/datasets/trec07p";
//        OfflineTraining filter = new OfflineTraining();
//        List<String> trainingSet = Files.readLines(new File(path + "/full/index"), Charset.defaultCharset());
//
//        System.out.println("Number of emails: " + trainingSet.size());
//
//        for (int i=0; i<trainingSet.size(); i++) {
//            if (i%1000 == 0)
//                System.out.println("Training set " + i);
//
//            String[] train = trainingSet.get(i).split("\\s+");
//
//            boolean isSpam = train[0].toLowerCase().trim().equals("spam");
//            String content = Files.toString(new File(path + "/data/" + train[1]), Charset.defaultCharset());
//
//            filter.train(content, isSpam);
//        }
//
//        filter.finalizeTraining();
//
//        boolean result = filter.saveTrainingAsString("/home/mayconbordin/Downloads/datasets/full_training.json");
//
//        if (!result) {
//            System.out.println("Not saved");
//        } else {
//            System.out.println("Saved");
//        }
        
        
        OfflineTraining filter = new OfflineTraining();
        filter.loadTrainingString("/home/mayconbordin/Downloads/datasets/full_training.json");

        System.out.println("Num words: " + filter.words.values().size());
        System.out.println("Num words: " + filter.words);
    }
}
