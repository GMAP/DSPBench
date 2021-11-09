package com.streamer.metrics;

import com.codahale.metrics.Gauge;

/**
 *
 * @author mayconbordin
 */
public class Progress implements Gauge<Double> {
    private double total;
    private double progress;

    public void setTotal(double total) {
        this.total = total;
    }
    
    public void setProgress(double progress) {
        this.progress = progress;
    }
    
    public Double getValue() {
        return (((double)progress*100.0)/(double)total);
    }
    
}
