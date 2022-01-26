package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang3.StringUtils;
import storm.applications.constants.WordCountConstants.Field;

public class SplitSentenceBolt extends AbstractBolt {
    private static final String splitregex = "\\W";
    
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple input) {
        String[] words = input.getString(0).split(splitregex);
        
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word));
        }
        
        collector.ack(input);
    }
    
}
