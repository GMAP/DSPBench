package org.dspbench.topology;

import org.dspbench.core.Source;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface ISourceAdapter extends IComponentAdapter<Source> {
    public void setTupleRate(int rate);
}
