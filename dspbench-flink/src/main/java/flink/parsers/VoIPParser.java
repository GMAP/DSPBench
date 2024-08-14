package flink.parsers;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.voipstream.CDRDataGenerator;
import flink.application.voipstream.CallDetailRecord;
import flink.util.RandomUtil;

public class VoIPParser extends Parser implements MapFunction<String, Tuple4<String, String, DateTime, CallDetailRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(VoIPParser.class);
    Configuration config;

    public VoIPParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple4<String, String, DateTime, CallDetailRecord> map(String value) throws Exception {
        super.initialize(config);
        super.incBoth();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        String[] temp = value.split(",");

        String[] cdrRgx = temp[3].split(";");
        DateTime CDRdate = formatter.parseDateTime(cdrRgx[2]);

        CallDetailRecord cdr = new CallDetailRecord();
        cdr.setCallingNumber(cdrRgx[0]);
        cdr.setCalledNumber(cdrRgx[1]);
        cdr.setAnswerTime(CDRdate);
        cdr.setCallDuration(Integer.parseInt(cdrRgx[3]));
        cdr.setCallEstablished(Boolean.parseBoolean(cdrRgx[4]));

        DateTime date = formatter.parseDateTime(temp[2]);

        return new Tuple4<String, String, DateTime, CallDetailRecord>(temp[0], temp[1], date, cdr);
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
