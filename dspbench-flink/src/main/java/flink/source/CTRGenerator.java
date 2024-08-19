package flink.source;

import java.util.UUID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.constants.ReinforcementLearnerConstants;

public class CTRGenerator extends RichParallelSourceFunction<Tuple2<String, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(CTRGenerator.class);
    private volatile boolean isRunning = true;

    private int roundNum = 1;
    private int eventCount = 0;
    private int maxRounds;

    public CTRGenerator(Configuration config) {
        maxRounds = config.getInteger(ReinforcementLearnerConstants.Conf.GENERATOR_MAX_ROUNDS,10000);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        while(roundNum <= maxRounds){
            String sessionID = UUID.randomUUID().toString();

            roundNum = roundNum + 1;
            eventCount = eventCount + 1;

            if (eventCount % 1000 == 0) {
                LOG.info("Generated {} events", eventCount);
            }
            ctx.collect(new Tuple2<String,Integer>(sessionID, roundNum));
        }
        
        isRunning = false;
    }
    
}
