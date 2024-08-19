package flink.source;

import java.util.Random;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;
import flink.application.voipstream.CDRDataGenerator;
import flink.application.voipstream.CallDetailRecord;
import flink.constants.VoIPStreamConstants;
import flink.util.RandomUtil;

public class CDRGenerator extends RichParallelSourceFunction<Tuple4<String, String, DateTime, CallDetailRecord>> {
    private volatile boolean isRunning = true;

    private String[] phoneNumbers;
    private int population;
    private double errorProb;
    private Random rand = new Random();

    public CDRGenerator(Configuration config) {
        this.population = config.getInteger(VoIPStreamConstants.Conf.GENERATOR_POPULATION, 50);
        this.errorProb  = config.getDouble(VoIPStreamConstants.Conf.GENERATOR_ERROR_PROB, 0.05);
        
        this.phoneNumbers = new String[population];
        
        for (int i=0; i<population; i++) {
            this.phoneNumbers[i] = CDRDataGenerator.phoneNumber("US", 11);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void run(SourceContext<Tuple4<String, String, DateTime, CallDetailRecord>> ctx) throws Exception {
        CallDetailRecord cdr = new CallDetailRecord();
        int count = 0;
        
        while (isRunning && count <= population) {
            cdr.setCallingNumber(pickNumber());
            cdr.setCalledNumber(pickNumber(cdr.getCallingNumber()));
            cdr.setAnswerTime(DateTime.now().plusMinutes(RandomUtil.randInt(0, 60)));
            cdr.setCallDuration(RandomUtil.randInt(0, 60 * 5));
            cdr.setCallEstablished(CDRDataGenerator.causeForTermination(errorProb) == CDRDataGenerator.TERMINATION_CAUSE_OK);

            count+=1;

            ctx.collect(new Tuple4<String,String,DateTime,CallDetailRecord>(cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), cdr));
        }

        isRunning = false;
    }

    private String pickNumber(String excluded) {
        String number = "";
        while (number.isEmpty() || number.equals(excluded)) {
            number = phoneNumbers[rand.nextInt(population)];
        }
        return number;
    }
    
    private String pickNumber() {
        return phoneNumbers[rand.nextInt(population)];
    }
    
}
