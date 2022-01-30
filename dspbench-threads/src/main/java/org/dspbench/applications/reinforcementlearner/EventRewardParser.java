package org.dspbench.applications.reinforcementlearner;

import org.dspbench.base.source.parser.Parser;
import org.dspbench.core.Values;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
        
        return List.of(new Values(fields[0], fields[1]));
    }
    
}
