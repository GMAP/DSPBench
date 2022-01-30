package org.dspbench.applications.voipstream;

import org.dspbench.base.source.generator.Generator;
import org.dspbench.core.Values;
import org.dspbench.applications.utils.RandomUtil;
import org.dspbench.utils.Configuration;
import java.util.Random;

import org.joda.time.DateTime;

/**
 *
 * @author mayconbordin
 */
public class CDRGenerator extends Generator {
    private String[] phoneNumbers;
    private long numCalls;
    private int population;
    private double errorProb;
    private Random rand = new Random();
    private long count = 0L;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        numCalls = config.getLong(VoIPSTREAMConstants.Config.GENERATOR_NUM_CALLS, 1000L);
        population = config.getInt(VoIPSTREAMConstants.Config.GENERATOR_POPULATION, 50);
        errorProb  = config.getDouble(VoIPSTREAMConstants.Config.GENERATOR_ERROR_PROB, 0.05);
        
        phoneNumbers = new String[population];
        
        for (int i=0; i<population; i++) {
            phoneNumbers[i] = CDRDataGenerator.phoneNumber("US", 11);
        }
    }
    
    @Override
    public Values generate() {
        String callingNumber = pickNumber();
        String calledNumber = pickNumber(callingNumber);
        DateTime answerTime = DateTime.now().plusMinutes(RandomUtil.randInt(0, 60));
        int callDuration = RandomUtil.randInt(0, 60 * 5);
        boolean callEstablished = (CDRDataGenerator.causeForTermination(errorProb) == CDRDataGenerator.TERMINATION_CAUSE_OK);
        
        Values values = new Values(callingNumber, calledNumber, answerTime, callDuration, callEstablished);
        values.setId(count++);
        return values;
    }

    @Override
    public boolean hasNext() {
        return count <= numCalls;
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
