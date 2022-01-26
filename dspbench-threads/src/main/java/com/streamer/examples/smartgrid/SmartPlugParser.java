package com.streamer.examples.smartgrid;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import com.streamer.utils.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * @author mayconbordin
 */
public class SmartPlugParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(SmartPlugParser.class);
    
    private static final int ID_FIELD           = 0;
    private static final int TIMESTAMP_FIELD    = 1;
    private static final int VALUE_FIELD        = 2;
    private static final int PROPERTY_FIELD     = 3;
    private static final int PLUG_ID_FIELD      = 4;
    private static final int HOUSEHOLD_ID_FIELD = 5;
    private static final int HOUSE_ID_FIELD     = 6;

    @Override
    public List<Values> parse(String input) {
        String[] fields = input.split(",");
        
        if (fields.length != 7)
            return null;
        
        try{
            String id = fields[ID_FIELD];
            long timestamp = Long.parseLong(fields[TIMESTAMP_FIELD]);
            double value = Double.parseDouble(fields[VALUE_FIELD]);
            int property = Integer.parseInt(fields[PROPERTY_FIELD]);
            String plugId = fields[PLUG_ID_FIELD];
            String householdId = fields[HOUSEHOLD_ID_FIELD];
            String houseId = fields[HOUSE_ID_FIELD];

            Values values = new Values(id, timestamp, value, property,
                                                   plugId, householdId, houseId);
            values.setId(HashUtils.murmurhash3(String.format("%s:%s", id, timestamp)));
            
            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing numeric value", ex);
        }
        
        return null;
    }
    
}
