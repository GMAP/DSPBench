package org.dspbench.applications.voipstream;

import com.google.common.collect.ImmutableList;

import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.spout.parser.Parser;
import org.dspbench.util.config.Configuration;
import org.dspbench.util.stream.StreamValues;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoipParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(VoipParser.class);

    @Override
    public List<StreamValues> parse(String str) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        String[] temp = str.split(",");

        String[] cdrRgx = temp[3].split(";");
        DateTime CDRdate = formatter.parseDateTime(cdrRgx[2]);

        CallDetailRecord cdr = new CallDetailRecord();
        cdr.setCallingNumber(cdrRgx[0]);
        cdr.setCalledNumber(cdrRgx[1]);
        cdr.setAnswerTime(CDRdate);
        cdr.setCallDuration(Integer.parseInt(cdrRgx[3]));
        cdr.setCallEstablished(Boolean.parseBoolean(cdrRgx[4]));

        DateTime date = formatter.parseDateTime(temp[2]);

        return ImmutableList.of(new StreamValues(temp[0], temp[1], date, cdr));
    }
}