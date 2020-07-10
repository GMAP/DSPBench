package com.streamer.core.impl;

import backtype.storm.topology.IRichSpout;
import com.streamer.core.Source;

/**
 *
 * @author mayconbordin
 */
public interface IStormSpout extends IRichSpout {
    public void setSource(Source source);
}
