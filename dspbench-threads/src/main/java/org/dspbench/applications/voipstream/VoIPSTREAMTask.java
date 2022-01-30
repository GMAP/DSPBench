package org.dspbench.applications.voipstream;

import org.dspbench.base.task.BasicTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;
import org.dspbench.partitioning.Fields;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.applications.voipstream.VoIPSTREAMConstants.PREFIX;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAMTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(VoIPSTREAMTask.class);

    private int varDetectThreads;
    private int ecrThreads;
    private int rcrThreads;
    private int encrThreads;
    private int ecr24Threads;
    private int ct24Threads;
    private int fofirThreads;
    private int urlThreads;
    private int globalAcdThreads;
    private int acdThreads;
    private int scorerThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        varDetectThreads = config.getInt(Config.VAR_DETECT_THREADS, 1);
        ecrThreads       = config.getInt(Config.ECR_THREADS, 1);
        rcrThreads       = config.getInt(Config.RCR_THREADS, 1);
        encrThreads      = config.getInt(Config.ENCR_THREADS, 1);
        ecr24Threads     = config.getInt(Config.ECR24_THREADS, 1);
        ct24Threads      = config.getInt(Config.CT24_THREADS, 1);
        fofirThreads     = config.getInt(Config.FOFIR_THREADS, 1);
        urlThreads       = config.getInt(Config.URL_THREADS, 1);
        acdThreads       = config.getInt(Config.ACD_THREADS, 1);
        scorerThreads    = config.getInt(Config.SCORER_THREADS, 1);
    }
    
    public void initialize() {
        Stream sourceStream = builder.createStream(Streams.CDR, new Schema().keys(Field.CALLING_NUM, Field.CALLED_NUM).fields(Field.ANSWER_TIME, Field.CALL_DURATION, Field.CALL_ESTABLISHED));

        Stream variationStream       = builder.createStream(Streams.VARIATIONS       , new Schema(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.CALL_DURATION, Field.NEW_CALLEE, Field.CALL_ESTABLISHED));
        Stream variationBackupStream = builder.createStream(Streams.VARIATIONS_BACKUP, new Schema(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.CALL_DURATION, Field.NEW_CALLEE, Field.CALL_ESTABLISHED));

        Stream ecrStream = builder.createStream(Streams.ESTABLISHED_CALL_RATES, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.RATE));
        Stream ecr24Stream = builder.createStream(Streams.ESTABLISHED_CALL_RATES_24, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.RATE));
        Stream rcrStream = builder.createStream(Streams.RECEIVED_CALL_RATES, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.RATE));
        Stream encrStream = builder.createStream(Streams.NEW_CALLEE_RATES, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.RATE));
        Stream ct24Stream = builder.createStream(Streams.CALL_TIME, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.CALLTIME));

        Stream fofirStream = builder.createStream(Streams.FOFIR_SCORE, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.SCORE));
        Stream urlStream = builder.createStream(Streams.URL_SCORE, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.SCORE));

        Stream globalAcdStream = builder.createStream(Streams.GLOBAL_ACD, new Schema(Field.ANSWER_TIME, Field.AVERAGE));
        Stream acdStream = builder.createStream(Streams.ACD, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.SCORE));
        Stream scorerStream = builder.createStream(Streams.SCORER, new Schema(Field.CALLING_NUM, Field.ANSWER_TIME, Field.SCORE));

        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, sourceStream);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.VARIATION_DETECTOR, new VariationDetectorOperator(), varDetectThreads);
        builder.groupByKey(Component.VARIATION_DETECTOR, sourceStream);
        builder.publish(Component.VARIATION_DETECTOR, variationStream);
        builder.publish(Component.VARIATION_DETECTOR, variationBackupStream);
        
        // Filters -----------------------------------------------------------//
        
        builder.setOperator(Component.ECR, new ECROperator(), ecrThreads);
        builder.groupBy(Component.ECR, variationStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.ECR, ecrStream);
        
        builder.setOperator(Component.RCR, new RCROperator(), rcrThreads);
        builder.groupBy(Component.RCR, variationStream, new Fields(Field.CALLING_NUM));
        builder.groupBy(Component.RCR, variationBackupStream, new Fields(Field.CALLED_NUM));
        builder.publish(Component.RCR, rcrStream);
        
        builder.setOperator(Component.ENCR, new ENCROperator(), encrThreads);
        builder.groupBy(Component.ENCR, variationStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.ENCR, encrStream);

        
        builder.setOperator(Component.ECR24, new ECR24Operator(), ecr24Threads);
        builder.groupBy(Component.ECR24, variationStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.ECR24, ecr24Stream);

        builder.setOperator(Component.CT24, new CTOperator(), ct24Threads);
        builder.groupBy(Component.CT24, variationStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.CT24, ct24Stream);
        
        
        // Modules
        
        builder.setOperator(Component.FOFIR, new FoFiROperator(), fofirThreads);
        builder.groupBy(Component.FOFIR, rcrStream, new Fields(Field.CALLING_NUM));
        builder.groupBy(Component.FOFIR, ecrStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.FOFIR, fofirStream);

        
        builder.setOperator(Component.URL, new URLOperator(), urlThreads);
        builder.groupBy(Component.URL, encrStream, new Fields(Field.CALLING_NUM));
        builder.groupBy(Component.URL, ecrStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.URL, urlStream);
        
        // the average must be global, so there must be a single instance doing that
        // perhaps a separate bolt, or if multiple bolts are used then a merger should
        // be employed at the end point.
        builder.setOperator(Component.GLOBAL_ACD, new GlobalACDBolt(), 1);
        builder.bcast(Component.GLOBAL_ACD, variationStream);
        builder.publish(Component.GLOBAL_ACD, globalAcdStream);
        
        builder.setOperator(Component.ACD, new ACDOperator(), acdThreads);
        builder.groupBy(Component.ACD, ecr24Stream, new Fields(Field.CALLING_NUM));
        builder.groupBy(Component.ACD, ct24Stream, new Fields(Field.CALLING_NUM));
        builder.bcast(Component.ACD, globalAcdStream);
        builder.publish(Component.ACD, acdStream);

        // Score
        builder.setOperator(Component.SCORER, new ScoreOperator(), scorerThreads);
        builder.groupBy(Component.SCORER, fofirStream, new Fields(Field.CALLING_NUM));
        builder.groupBy(Component.SCORER, urlStream, new Fields(Field.CALLING_NUM));
        builder.groupBy(Component.SCORER, acdStream, new Fields(Field.CALLING_NUM));
        builder.publish(Component.SCORER, scorerStream);

        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, scorerStream);
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
