package com.streamer.topology.impl;

import backtype.storm.topology.SpoutDeclarer;
import com.streamer.core.Source;
import com.streamer.core.hook.Hook;
import com.streamer.core.impl.IStormSpout;
import com.streamer.topology.ISourceAdapter;

/**
 *
 * @author mayconbordin
 */
public class StormSourceAdapter implements ISourceAdapter {
    private Source source;
    private SpoutDeclarer declarer;
    private IStormSpout spout;
    private int rate;

    public void setSpout(IStormSpout spout) {
        this.spout = spout;
    }

    public void setComponent(Source source) {
        this.source = source;
        spout.setSource(source);
    }

    public Source getComponent() {
        return source;
    }

    public void setTupleRate(int rate) {
        this.rate = rate;
    }

    public SpoutDeclarer getDeclarer() {
        return declarer;
    }

    public void setDeclarer(SpoutDeclarer declarer) {
        this.declarer = declarer;
    }

    public IStormSpout getSpout() {
        return spout;
    }

    public void addComponentHook(Hook hook) {
        source.addHook(hook);
    }
}
