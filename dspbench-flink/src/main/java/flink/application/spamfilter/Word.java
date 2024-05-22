package flink.application.spamfilter;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Word implements Serializable{
    private static final long serialVersionUID = 1667802979041340740L;

    @JsonProperty("word")
    private String word;    // The String itself
    @JsonProperty("count_bad")
    private int countBad;   // The total times it appears in "bad" messages
    @JsonProperty("count_good")
    private int countGood;  // The total times it appears in "good" messages
    @JsonProperty("r_bad")
    private float rBad;     // bad count / total bad words
    @JsonProperty("r_good")
    private float rGood;    // good count / total good words
    @JsonProperty("p_spam")
    private float pSpam;    // probability this word is Spam

    public Word() {
    }

    // Create a word, initialize all vars to 0
    public Word(String s) {
        word = s;
        countBad = 0;
        countGood = 0;
        rBad = 0.0f;
        rGood = 0.0f;
        pSpam = 0.0f;
    }

    private Word(String word, int countBad, int countGood, float rBad, float rGood, float pSpam) {
        this.word = word;
        this.countBad = countBad;
        this.countGood = countGood;
        this.rBad = rBad;
        this.rGood = rGood;
        this.pSpam = pSpam;
    }

    // Increment bad counter
    public void countBad() {
        countBad++;
    }

    // Increment good counter
    public void countGood() {
        countGood++;
    }

    public void countBad(int increment) {
        countBad += increment;
    }

    // Increment good counter
    public void countGood(int increment) {
        countGood += increment;
    }

    public void calcProbs(long badTotal, long goodTotal) {
        calcBadProb(badTotal);
        calcGoodProb(goodTotal);
        finalizeProb();
    }

    // Computer how often this word is bad
    public void calcBadProb(long total) {
        if (total > 0)
            rBad = countBad / (float) total;
    }

    // Computer how often this word is good
    public void calcGoodProb(long total) {
        if (total > 0)
            rGood = 2*countGood / (float) total; // multiply 2 to help fight against false positives (via Graham)
    }

    // Implement bayes rules to computer how likely this word is "spam"
    public void finalizeProb() {
        if (rGood + rBad > 0)
            pSpam = rBad / (rBad + rGood);
        if (pSpam < 0.01f)
            pSpam = 0.01f;
        else if (pSpam > 0.99f)
            pSpam = 0.99f;
    }

    // The "interesting" rating for a word is
    // How different from 0.5 it is
    @JsonIgnore
    public float interesting() {
        return Math.abs(0.5f - pSpam);
    }

    public String getWord() {
        return word;
    }

    public int getCountBad() {
        return countBad;
    }

    public int getCountGood() {
        return countGood;
    }

    public float getrBad() {
        return rBad;
    }

    public float getrGood() {
        return rGood;
    }

    public float getpSpam() {
        return pSpam;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public void setCountBad(int countBad) {
        this.countBad = countBad;
    }

    public void setCountGood(int countGood) {
        this.countGood = countGood;
    }

    public void setrBad(float rBad) {
        this.rBad = rBad;
    }

    public void setrGood(float rGood) {
        this.rGood = rGood;
    }

    public void setpSpam(float pSpam) {
        this.pSpam = pSpam;
    }

    @Override
    public String toString() {
        return "Word{" + "word=" + word + ", countBad=" + countBad + ", countGood=" + countGood + ", rBad=" + rBad + ", rGood=" + rGood + ", pSpam=" + pSpam + '}';
    }
}
