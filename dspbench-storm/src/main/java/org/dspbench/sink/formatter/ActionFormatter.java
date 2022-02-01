package org.dspbench.sink.formatter;

import org.apache.storm.tuple.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants.Field;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ActionFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        String eventID   = tuple.getStringByField(Field.EVENT_ID);
        String[] actions = (String[]) tuple.getValueByField(Field.ACTIONS);
        
        String actionList = actions.length > 1 ? StringUtils.join(actions) : actions[0];
 
        return eventID + "," + actionList;
    }
    
}
