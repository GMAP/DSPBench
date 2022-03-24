package org.dspbench.applications.reinforcementlearner;

import org.dspbench.base.sink.BaseSink;
import org.dspbench.core.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class ActionSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ActionSink.class);

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public void process(Tuple tuple) {
        String[] actions = (String[]) tuple.getValue(ReinforcementLearnerConstants.Field.ACTIONS);

        LOG.info("ACTIONS = {}", actions);
        LOG.info("Adding element {} to the queue", actions[0]);
        SharedQueue.QUEUE.add(actions[0]);
    }
}