package flink.application.voipstream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.AbstractApplication;
import flink.application.adanalytics.AdEvent;
import flink.constants.VoIPStreamConstants;
import flink.parsers.LearnerParser;
import flink.parsers.VoIPParser;
import flink.source.CDRGenerator;

public class VoIPStream extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(VoIPStream.class);

    private int parserThreads;
    private int varDetectThreads;
    private int ecrThreads;
    private int rcrThreads;
    private int encrThreads;
    private int ecr24Threads;
    private int ct24Threads;
    private int fofirThreads;
    private int urlThreads;
    private int acdThreads;
    private int scorerThreads;

    public VoIPStream(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInteger(VoIPStreamConstants.Conf.PARSER_THREADS, 1);
        varDetectThreads = config.getInteger(VoIPStreamConstants.Conf.VAR_DETECT_THREADS, 1);
        ecrThreads       = config.getInteger(VoIPStreamConstants.Conf.ECR_THREADS, 1);
        rcrThreads       = config.getInteger(VoIPStreamConstants.Conf.RCR_THREADS, 1);
        encrThreads      = config.getInteger(VoIPStreamConstants.Conf.ENCR_THREADS, 1);
        ecr24Threads     = config.getInteger(VoIPStreamConstants.Conf.ECR24_THREADS, 1);
        ct24Threads      = config.getInteger(VoIPStreamConstants.Conf.CT24_THREADS, 1);
        fofirThreads     = config.getInteger(VoIPStreamConstants.Conf.FOFIR_THREADS, 1);
        urlThreads       = config.getInteger(VoIPStreamConstants.Conf.URL_THREADS, 1);
        acdThreads       = config.getInteger(VoIPStreamConstants.Conf.ACD_THREADS, 1);
        scorerThreads    = config.getInteger(VoIPStreamConstants.Conf.SCORER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> source = createSource(); // 
        //CDRGenerator source = new CDRGenerator(config);
        
        // Parser
        DataStream<Tuple4<String, String, DateTime, CallDetailRecord>> data =  source.flatMap(new VoIPParser(config)).setParallelism(parserThreads); // env.addSource(source); 
        
        // Processs
        DataStream<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>> variationDetector = data.keyBy(
            new KeySelector<Tuple4<String, String, DateTime, CallDetailRecord>,Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> getKey(Tuple4<String, String, DateTime, CallDetailRecord> value){
                    return Tuple2.of(value.f0, value.f1);
                }
            }
        ).flatMap(new VariationDetector(config)).setParallelism(varDetectThreads);
        
        //Don't know if a second is necessary, but is here if needed
        /*DataStream<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>> variationDetectorBKP = data.keyBy(
            new KeySelector<Tuple4<String, String, DateTime, CallDetailRecord>,Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> getKey(Tuple4<String, String, DateTime, CallDetailRecord> value){
                    return Tuple2.of(value.f0, value.f1);
                }
            }
        ).flatMap(new VariationDetector(config)).setParallelism(varDetectThreads);*/
        
        // Filters
        DataStream<Tuple4<String, Long, Double, CallDetailRecord>> ECR = variationDetector.keyBy(value -> value.f0)
            .flatMap(new ECR(config, "ecr")).setParallelism(ecrThreads);

        DataStream<Tuple4<String, Long, Double, CallDetailRecord>> RCR = variationDetector.keyBy(value -> value.f1)
            .connect(variationDetector.keyBy(value -> value.f0))
            .flatMap(new RCR(config)).setParallelism(rcrThreads);
        
        DataStream<Tuple4<String, Long, Double, CallDetailRecord>> ENCR = variationDetector.keyBy(value -> value.f0)
            .flatMap(new ENCR(config)).setParallelism(encrThreads);

        DataStream<Tuple5<String, Long, Double, CallDetailRecord, String>> ECR24 = variationDetector.keyBy(value -> value.f0)
            .flatMap(new ECRCT24(config, "ecr24")).setParallelism(ecr24Threads);

        DataStream<Tuple5<String, Long, Double, CallDetailRecord, String>> CT24 = variationDetector.keyBy(value -> value.f0)
            .flatMap(new ECRCT24(config, "ct24")).setParallelism(ct24Threads);

        // Modules
        DataStream<Tuple5<String, Long, Double, CallDetailRecord, String>> FoFiR = RCR.keyBy(value -> value.f0)
            .connect(ECR.keyBy(value -> value.f0))
            .flatMap(new FoFiR(config)).setParallelism(fofirThreads);

        DataStream<Tuple5<String, Long, Double, CallDetailRecord, String>> URL = ENCR.keyBy(value -> value.f0)
            .connect(ECR.keyBy(value -> value.f0))
            .flatMap(new URL(config)).setParallelism(urlThreads);

        DataStream<Tuple2<Long, Double>> GlobalACD = variationDetector.flatMap(new GlobalACD(config)).setParallelism(1);

        DataStream<Tuple5<String, Long, Double, CallDetailRecord, String>> ACD = (ECR24.keyBy(value -> value.f0).union(CT24.keyBy(value -> value.f0)))
            .connect(GlobalACD.broadcast()).flatMap(new ACD(config)).setParallelism(acdThreads);

        // Score
        DataStream<Tuple4<String, Long, Double, CallDetailRecord>> Scorer = FoFiR.keyBy(value -> value.f0)
            .union(URL.keyBy(value -> value.f0), ACD.keyBy(value -> value.f0))
            .flatMap(new Scorer(config)).setParallelism(scorerThreads);

        // Sink
        createSinkVS(Scorer);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return VoIPStreamConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
