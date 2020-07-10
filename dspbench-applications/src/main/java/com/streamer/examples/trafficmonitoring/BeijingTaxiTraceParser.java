package com.streamer.examples.trafficmonitoring;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BeijingTaxiTraceParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(BeijingTaxiTraceParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    
    private static final int ID_FIELD    = 0;
    private static final int NID_FIELD   = 1;
    private static final int DATE_FIELD  = 2;
    private static final int LAT_FIELD   = 3;
    private static final int LON_FIELD   = 4;
    private static final int SPEED_FIELD = 5;
    private static final int DIR_FIELD   = 6;
    
    @Override
    public List<Values> parse(String input) {
        String[] fields = input.replace("\"", "").split(",");
        
        if (fields.length != 7)
            return null;
        
        try {
            String carId  = fields[ID_FIELD];
            DateTime date = formatter.parseDateTime(fields[DATE_FIELD]);
            boolean occ   = true;
            double lat    = Double.parseDouble(fields[LAT_FIELD]);
            double lon    = Double.parseDouble(fields[LON_FIELD]);
            int speed     = ((Double)Double.parseDouble(fields[SPEED_FIELD])).intValue();
            int bearing   = Integer.parseInt(fields[DIR_FIELD]);           
            int id        = String.format("%s:%s", carId, date.toString()).hashCode();
            
            Values values = new Values(carId, date.toDate(), occ, speed, bearing, lat, lon);
            values.setId(id);
            
            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Error parsing date/time value", ex);
        }
        
        return null;
    }
    
}