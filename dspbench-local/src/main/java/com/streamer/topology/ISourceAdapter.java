package com.streamer.topology;

import com.streamer.core.Source;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface ISourceAdapter extends IComponentAdapter<Source> {
    public void setTupleRate(int rate);
}
