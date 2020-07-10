package com.streamer.topology;

import com.streamer.core.Operator;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.core.Schema;
import com.streamer.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface ComponentFactory {
    public Stream createStream(String name, Schema schema);
    public IOperatorAdapter createOperatorAdapter(String name, Operator operator);
    public ISourceAdapter createSourceAdapter(String name, Source source);
    public Topology createTopology(String name);
    
    //public void setMetrics(MetricRegistry metrics);
    public void setConfiguration(Configuration configuration);
}
