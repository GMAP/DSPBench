package flink.parsers;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.voipstream.CDRDataGenerator;
import flink.application.voipstream.CallDetailRecord;
import flink.util.Configurations;
import flink.util.Metrics;
import flink.util.RandomUtil;

public class VoIPParser extends RichFlatMapFunction<String, Tuple4<String, String, DateTime, CallDetailRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(VoIPParser.class);
    Configuration config;
    Metrics metrics = new Metrics();

    public VoIPParser(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple4<String, String, DateTime, CallDetailRecord>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        String[] temp = input.split(",");

        String[] cdrRgx = temp[3].split(";");
        DateTime CDRdate = formatter.parseDateTime(cdrRgx[2]);

        CallDetailRecord cdr = new CallDetailRecord();
        cdr.setCallingNumber(cdrRgx[0]);
        cdr.setCalledNumber(cdrRgx[1]);
        cdr.setAnswerTime(CDRdate);
        cdr.setCallDuration(Integer.parseInt(cdrRgx[3]));
        cdr.setCallEstablished(Boolean.parseBoolean(cdrRgx[4]));

        DateTime date = formatter.parseDateTime(temp[2]);

        out.collect(new Tuple4<String, String, DateTime, CallDetailRecord>(temp[0], temp[1], date, cdr));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
