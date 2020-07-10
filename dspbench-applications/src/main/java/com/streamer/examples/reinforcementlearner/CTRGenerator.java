package com.streamer.examples.reinforcementlearner;

import com.streamer.base.source.generator.Generator;
import com.streamer.core.Values;
import com.streamer.examples.voipstream.VoIPSTREAMConstants;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 *
 * @author mayconbordin
 */
public class CTRGenerator extends Generator {
    private static final Logger LOG = LoggerFactory.getLogger(CTRGenerator.class);

    private long roundNum = 1;
    private long eventCount = 0;
    private long maxRounds;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);

        maxRounds = config.getLong(ReinforcementLearnerConstants.Config.GENERATOR_MAX_ROUNDS);
    }

    @Override
    public boolean hasNext() {
        return roundNum <= maxRounds;
    }

    @Override
    public Values generate() {
        String sessionID = UUID.randomUUID().toString();

        roundNum = roundNum + 1;
        eventCount = eventCount + 1;

        if (eventCount % 1000 == 0) {
            LOG.info("Generated {} events", eventCount);
        }

        Values values = new Values(sessionID, roundNum);
        values.setId(eventCount);
        values.setStreamId(ReinforcementLearnerConstants.Streams.EVENTS);

        return values;
    }
}
