package org.dspbench.applications.microbenchmarks;

import org.dspbench.base.task.BasicTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import static org.dspbench.applications.microbenchmarks.MicroBenchmarksConstants.*;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class ThroughputTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(ThroughputTask.class);

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
    }

    public void initialize() {
        Stream data = builder.createStream(Streams.DATA, new Schema(Field.DATA));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, data);
        //builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, data);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
