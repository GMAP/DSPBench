package com.streamer.topology;

import com.streamer.core.Operator;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface IOperatorAdapter extends IComponentAdapter<Operator> {
    public void setTimeInterval(long interval, TimeUnit timeUnit);
    public boolean hasTimer();
    public long getTimeIntervalMillis();
}
