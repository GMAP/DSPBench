package org.dspbench.applications.voipstream;

import org.dspbench.base.constants.BaseConstants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface VoIPSTREAMConstants extends BaseConstants {
    String PREFIX = "vs";
    
    interface Config extends BaseConfig {
        String GENERATOR_NUM_CALLS = "vs.generator.num_calls";
        String GENERATOR_POPULATION = "vs.generator.population";
        String GENERATOR_ERROR_PROB = "vs.generator.error_prob";
        
        String FILTER_NUM_ELEMENTS = "vs.%s.num_elements";
        String FILTER_BUCKETS_PEL = "vs.%s.buckets_per_element";
        String FILTER_BUCKETS_PWR = "vs.%s.buckets_per_word";
        String FILTER_BETA = "vs.%s.beta";
        
        String SCORE_THRESHOLD_MIN = "vs.%s.threshold.min";
        String SCORE_THRESHOLD_MAX = "vs.%s.threshold.max";
        
        String ACD_DECAY_FACTOR = "vs.acd.decay_factor";
        
        String FOFIR_WEIGHT = "vs.fofir.weight";
        String URL_WEIGHT = "vs.url.weight";
        String ACD_WEIGHT = "vs.acd.weight";
        
        String VAR_DETECT_APROX_SIZE = "vs.variation.aprox_size";
        String VAR_DETECT_ERROR_RATE = "vs.variation.error_rate";
        
        String VAR_DETECT_THREADS = "vs.variation.threads";

        String ECR_THREADS = "vs.ecr.threads";

        String RCR_THREADS = "vs.rcr.threads";

        String ENCR_THREADS = "vs.encr.threads";

        String ECR24_THREADS = "vs.ecr24.threads";

        String CT24_THREADS = "vs.ct24.threads";

        String FOFIR_THREADS = "vs.fofir.threads";

        String URL_THREADS = "vs.url.threads";

        String ACD_THREADS = "vs.acd.threads";

        String SCORER_THREADS = "vs.scorer.threads";
    }
    
    interface Component extends BaseComponent {
        String SOURCE = "source";
        String VARIATION_DETECTOR = "VariationDetector";
        String RCR = "RCRFilter";
        String ECR = "ECRFilter";
        String ENCR = "ENCRFilter";
        String ECR24 = "ECR24Filter";
        String CT24 = "CT24Filter";
        String FOFIR = "FoFiRModule";
        String URL = "URLModule";
        String ACD = "ACDModule";
        String GLOBAL_ACD = "GlobalACDModule";
        String SCORER = "Scorer";
    }
    
    interface Field {
        String CALLING_NUM = "callingNumber";
        String CALLED_NUM = "calledNumber";
        String TIMESTAMP = "timestamp";
        String CALL_DURATION = "callDuration";
        String CALL_ESTABLISHED = "callEstablished";
        String SCORE = "score";
        String RECORD = "record";
        String AVERAGE = "average";
        String CALLTIME = "calltime";
        String NEW_CALLEE = "newCallee";
        String RATE = "rate";
        String ANSWER_TIME = "answerTime";
    }
    
    interface Streams extends BaseStream {
        String CDR = "cdrStream";
        String VARIATIONS = "variationsStream";
        String VARIATIONS_BACKUP = "variationsBackupStream";
        String ESTABLISHED_CALL_RATES = "establishedCallRatesStream";
        String ESTABLISHED_CALL_RATES_24 = "establishedCallRates24Stream";
        String RECEIVED_CALL_RATES = "receivedCallRatesStream";
        String NEW_CALLEE_RATES = "newCalleeRatesStream";
        String CALL_TIME = "callTimeStream";
        String FOFIR_SCORE = "fofirScoreStream";
        String URL_SCORE = "urlScoreStream";
        String GLOBAL_ACD = "globalAcdStream";
        String ACD = "acdStream";
        String SCORER = "scorerStream";
    }
}
