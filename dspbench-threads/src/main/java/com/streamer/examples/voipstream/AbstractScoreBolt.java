package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;
import com.streamer.utils.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractScoreBolt extends Operator {
    protected static enum Source {
        ECR, RCR, ECR24, ENCR, CT24, VD, FOFIR, ACD, GACD, URL, NONE
    }
    
    protected double thresholdMin;
    protected double thresholdMax;
    protected String configPrefix;
    protected Map<String, Entry> map;

    public AbstractScoreBolt(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);

        map = new HashMap<String, Entry>();

        // parameters
        if (configPrefix != null) {
            thresholdMin = config.getDouble(String.format(Config.SCORE_THRESHOLD_MIN, configPrefix));
            thresholdMax = config.getDouble(String.format(Config.SCORE_THRESHOLD_MAX, configPrefix));
        }

    }
    
    protected abstract Source[] getFields();
    
    protected static Source parseComponentId(String id) {
        if (id.equals(Component.VARIATION_DETECTOR))
            return Source.VD;
        else if (id.equals(Component.ECR24))
            return Source.ECR24;
        else if (id.equals(Component.CT24))
            return Source.CT24;
        else if (id.equals(Component.ECR))
            return Source.ECR;
        else if (id.equals(Component.RCR))
            return Source.RCR;
        else if (id.equals(Component.ENCR))
            return Source.ENCR;
        else if (id.equals(Component.ACD))
            return Source.ACD;
        else if (id.equals(Component.GLOBAL_ACD))
            return Source.GACD;
        else if (id.equals(Component.URL))
            return Source.URL;
        else if (id.equals(Component.FOFIR))
            return Source.FOFIR;
        else
            return Source.NONE;
    }
    
    protected static double score(double v1, double v2, double vi) {
        double score = vi/(v1 + (v2-v1));
        if (score < 0) score = 0; 
        if (score > 1) score = 1;
        return score;
    }
    
    protected class Entry {
        public Source[] fields;
        public double[] values;

        public Entry() {
            this.fields = getFields();
            
            values = new double[fields.length];
            Arrays.fill(values, Double.NaN);
        }

        public void set(Source src, double rate) {
            values[pos(src)] = rate;
        }
        
        public double get(Source src) {
            return values[pos(src)];
        }
        
        public boolean isFull() {
            for (double value : values)
                if (Double.isNaN(value))
                    return false;
            return true;
        }
        
        private int pos(Source src) {
            for (int i=0; i<fields.length; i++)
                if (fields[i] == src)
                    return i;
            return -1;
        }

        public double[] getValues() {
            return values;
        }

        @Override
        public String toString() {
            return "Entry{fields=" + Arrays.toString(fields) + ", values=" + Arrays.toString(values) + '}';
        }
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    @Override
    public com.streamer.core.Component copy() {
        com.streamer.core.Component newInstance = super.copy();
        ((AbstractScoreBolt) newInstance).setConfigPrefix(configPrefix);

        return newInstance;
    }
}