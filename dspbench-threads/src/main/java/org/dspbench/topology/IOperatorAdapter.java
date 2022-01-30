package org.dspbench.topology;

import org.dspbench.core.Operator;

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
