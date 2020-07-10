package com.streamer.topology.impl;

import backtype.storm.topology.BoltDeclarer;
import com.streamer.core.Operator;
import com.streamer.core.hook.Hook;
import com.streamer.core.impl.StormBolt;
import com.streamer.topology.IOperatorAdapter;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StormOperatorAdapter implements IOperatorAdapter {
    private Operator operator;
    private BoltDeclarer declarer;
    private StormBolt bolt;
    private long interval = -1;
    
    public StormOperatorAdapter() {
        bolt = new StormBolt();
    }

    public void setComponent(Operator operator) {
        this.operator = operator;
        bolt.setOperator(operator);
    }

    public Operator getComponent() {
        return operator;
    }

    public void setDeclarer(BoltDeclarer declarer) {
        this.declarer = declarer;
    }

    public BoltDeclarer getDeclarer() {
        return declarer;
    }

    public StormBolt getBolt() {
        return bolt;
    }

    public void addComponentHook(Hook hook) {
        operator.addHook(hook);
    }

    public void setTimeInterval(long interval, TimeUnit timeUnit) {
        this.interval = timeUnit.toSeconds(interval);
        bolt.setTimeInterval(this.interval);
    }

    public boolean hasTimer() {
        return interval != -1;
    }

    public long getTimeIntervalMillis() {
        return TimeUnit.SECONDS.toMillis(interval);
    }
}
