package com.streamer.core;

import com.streamer.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Source extends Component {
    
    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
    }
    
    @Override
    public void onDestroy() {
        super.onDestroy();
    }
    
    public abstract boolean hasNext();
    public abstract void nextTuple();
}
