package com.streamer.examples.reinforcementlearner;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class EventRewardParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(EventRewardParser.class);
    
    @Override
    public List<Values> parse(String str) {
        if (StringUtils.isBlank(str))
            return null;
        
        String[] fields = str.split(",");
        
        if (fields.length != 2) {
            LOG.warn("Wrong number of fields for event/reward: {}", str);
            return null;
        }
        
        return ImmutableList.of(new Values(fields[0], fields[1]));
    }
    
}
