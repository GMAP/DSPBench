package spark.streaming.model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NLPResource implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(NLPResource.class);
    
    public static final long serialVersionUID = 42L;
    private static List<String> stopWords;
    private static Set<String> posWords;
    private static Set<String> negWords;

    private static void readResource(String path, Collection<String> collection) {
        BufferedReader rd = null;
        
        try {
            rd = new BufferedReader(new InputStreamReader(NLPResource.class.getResourceAsStream(path)));
            String line = null;
            
            while ((line = rd.readLine()) != null) {
                collection.add(line);
            }
        } catch (IOException ex) {
            LOG.error("IO error while initializing", ex);
        } finally {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
    }

    public static List<String> getStopWords() {
        if (stopWords == null) {
            stopWords = new ArrayList<>();
            readResource("/stop-words.txt", stopWords);
        }
        
        return stopWords;
    }
    
    public static Set<String> getPosWords() {
        if (posWords == null) {
            posWords = new HashSet<>();
            readResource("/pos-words.txt", posWords);
        }
        
        return posWords;
    }
    
    public static Set<String> getNegWords() {
        if (negWords == null) {
            negWords = new HashSet<>();
            readResource("/neg-words.txt", negWords);
        }
        
        return negWords;
    }
}